import { type Piscina } from "piscina";
import { type Operator, type OperatorArgs, type OperatorOutput, OperatorArgsSchema, OperatorOutputSchema } from "./operator.js";

/**
 * A lightweight DAG executor that wires Operators together via named promises.
 *
 * Each operator declares schema-level input/output names. The DAG maps these to
 * "global" task names (via the operator's name maps) and connects them with
 * promises: an operator awaits its input promises, runs exec(), then resolves
 * its output promises so downstream operators can proceed.
 */
export class LightDag {
    private readonly operators: Map<string, Operator>;
    private readonly task_names: Set<string>;
    private debug: boolean;
    private timeout?: number | undefined;

    constructor(operators: Operator[], debug?: boolean, timeout?: number) {
        this.debug = debug ?? false;
        this.timeout = timeout;
        this.operators = new Map<string, Operator>();
        this.task_names = new Set<string>();
        // Discover all global task names by scanning each operator's schemas
        // and applying name maps (schema name → global name).
        for (const op of operators) {
            if (this.operators.has(op.name))
                throw new Error(`Duplicate operator name: "${op.name}"`);
            for (const name of Object.keys(op.input_schema.shape).map(n => op.input_map?.[n] ?? n))
                this.task_names.add(name);
            for (const [, outputs] of Object.entries(op.output_schemas))
                for (const name of Object.keys(outputs.shape).map(n => op.output_map?.[n] ?? n))
                    this.task_names.add(name);
            this.operators.set(op.name, op);
        }
    }

    public setDebug(enabled: boolean): void {
        this.debug = enabled;
    }

    public setTimeout(timeout: number): void {
        this.timeout = timeout;
    }

    private log(...args: unknown[]): void {
        if (this.debug)
            console.log("[LightDag]", ...args);
    }

    /**
     * Execute the DAG.
     * @param inputs   Initial values to inject, keyed by global task name.
     * @param outputs  Global task names whose resolved values to return.
     * @param context  Shared context (e.g. API keys) — operators select from it via requires_schema.
     * @param options  Runtime options — operators select from it via options_schema.
     * @param pool     Optional Piscina worker pool for operators with a worker script exec.
     */
    public async run(
        inputs: Record<string, unknown>,
        outputs: string[],
        context?: Record<string, unknown>,
        options?: Record<string, unknown>,
        pool?: Piscina
    ) {
        // Debug-only: warn about tasks that are produced but never consumed, or
        // consumed but never produced (likely a wiring mistake).
        if (this.debug) {
            const consumed = new Set<string>();
            const produced = new Set<string>();
            for (const op of this.operators.values()) {
                for (const name of Object.keys(op.input_schema.shape))
                    consumed.add(op.input_map?.[name] ?? name);
                for (const branchSchema of Object.values(op.output_schemas))
                    for (const name of Object.keys(branchSchema.shape))
                        produced.add(op.output_map?.[name] ?? name);
            }
            const provided_inputs = new Set(Object.keys(inputs));
            const requested_outputs = new Set(outputs);
            for (const name of produced) {
                if (!consumed.has(name) && !requested_outputs.has(name))
                    console.warn(`[LightDag] Warning: task "${name}" is produced but never consumed by any operator or requested as output`);
            }
            for (const name of consumed) {
                if (!produced.has(name) && !provided_inputs.has(name))
                    console.warn(`[LightDag] Warning: task "${name}" is consumed but never produced by any operator or provided as input`);
            }
        }
        // Create a promise + resolve/reject pair for every global task name.
        // Operators await input promises and resolve output promises when done.
        const tasks = new Map<string, Promise<unknown>>();
        const resolves = new Map<string, { resolve: (value: unknown) => void, reject: (reason: unknown) => void }>();
        for (const task of this.task_names) {
            const { promise, resolve, reject } = Promise.withResolvers<unknown>();
            promise.catch(() => { }); // Prevent unhandled rejection for unused branches
            tasks.set(task, promise);
            resolves.set(task, { resolve, reject });
        }
        this.log("run() started");
        let timeout_id: NodeJS.Timeout | undefined;
        if (this.timeout)
            timeout_id = setTimeout(() => {
                const err = new Error(`[LightDag] Error: Execution timeout: ${this.timeout}ms`);
                for (const { reject } of resolves.values())
                    reject(err);
            }, this.timeout);
        try {
            // Kick off all operators concurrently — they'll block on their input promises.
            for (const op_name of this.operators.keys())
                this.runNode(op_name, tasks, resolves, context, options, pool);
            // Resolve the initial input tasks to unblock the first wave of operators.
            for (const [input_name, input_value] of Object.entries(inputs)) {
                const entry = resolves.get(input_name);
                if (!entry)
                    throw new Error(`Input "${input_name}" not found`);

                entry.resolve(input_value);
            }
            // Wait for the requested output tasks to resolve and collect results.
            const output_promises = outputs.map((output_name) => {
                const task = tasks.get(output_name);
                if (!task)
                    throw new Error(`Requested output "${output_name}" does not exist in the DAG`);
                return task;
            });
            const resolved_outputs = await Promise.all(output_promises);
            const output_values: Record<string, unknown> = {};
            for (let i = 0; i < outputs.length; i++) {
                output_values[outputs[i]!] = resolved_outputs[i];
            }
            this.log("run() completed");
            return output_values;
        } finally {
            if (timeout_id)
                clearTimeout(timeout_id);
        }
    }

    /**
     * Run a single operator node within the DAG.
     *
     * Lifecycle:
     * 1. Collect all global output names (for error rejection if exec fails).
     * 2. Await input promises from the DAG task map.
     * 3. Remap context/options from global keys to schema keys.
     * 4. Call exec (in-process or via worker pool).
     * 5. Validate the output branch and resolve output promises.
     */
    private async runNode(
        name: string,
        tasks: Map<string, Promise<unknown>>,
        resolves: Map<string, { resolve: (value: unknown) => void, reject: (reason: unknown) => void }>,
        context?: Record<string, unknown>,
        options?: Record<string, unknown>,
        pool?: Piscina
    ): Promise<void> {
        let all_global_output_names: string[] = [];
        try {
            const op = this.operators.get(name);
            if (!op) {
                throw new Error(`Node ${name} not found`);
            }
            // Pre-collect all output names so we can reject them on error.
            for (const branchSchema of Object.values(op.output_schemas))
                for (const key of Object.keys(branchSchema.shape))
                    all_global_output_names.push(op.output_map?.[key] ?? key);

            // Map schema input names to global DAG task names and await them.
            const schema_input_names = Object.keys(op.input_schema.shape);
            const global_input_names = schema_input_names.map(n => op.input_map?.[n] ?? n);
            // Remap context and options from global keys to schema keys,
            // then validate with the operator's schemas.
            const op_requires: Record<string, unknown> = {};
            const schema_requires_names = Object.keys(op.requires_schema.shape);
            const global_requires_names = schema_requires_names.map(n => op.requires_map?.[n] ?? n);
            for (let i = 0; i < schema_requires_names.length; i++) {
                op_requires[schema_requires_names[i]!] = context?.[global_requires_names[i]!];
            }
            const op_options: Record<string, unknown> = {};
            const schema_options_names = Object.keys(op.options_schema.shape);
            const global_options_names = schema_options_names.map(n => op.options_map?.[n] ?? n);
            for (let i = 0; i < schema_options_names.length; i++) {
                op_options[schema_options_names[i]!] = options?.[global_options_names[i]!];
            }

            const parsed_requires = op.requires_schema.parse(op_requires);
            const parsed_options = op.options_schema.parse(op_options);

            const input_results = await Promise.all(
                global_input_names.map(global_name => {
                    const task = tasks.get(global_name);
                    if (!task)
                        throw new Error(`Task "${global_name}" not found for node "${name}"`);
                    return task;
                })
            );
            const inputs: Record<string, unknown> = {};
            for (let i = 0; i < schema_input_names.length; i++) {
                inputs[schema_input_names[i]!] = input_results[i];
            }
            this.log(`[${name}] START — inputs resolved: [${global_input_names.join(", ")}]`);
            const parsed_inputs = op.input_schema.parse(inputs);


            let op_output: OperatorOutput;
            const op_input_arg: OperatorArgs = { inputs: parsed_inputs, requires: parsed_requires, options: parsed_options };
            if (typeof op.exec === "string") {
                if (!pool)
                    throw new Error(`Worker pool not initialized for node ${name}`);
                op_output = await pool.run(op_input_arg, { filename: op.exec });
            } else {
                op_output = await op.exec(op_input_arg);
            }
            op_output = OperatorOutputSchema.parse(op_output);
            if (!Object.hasOwn(op.output_schemas, op_output.branch))
                throw new Error(`Branch "${op_output.branch}" not found for node "${name}"`);

            // Validate outputs against the branch schema, then resolve the
            // corresponding global task promises so downstream operators unblock.
            const parsed_output = op.output_schemas[op_output.branch]!.parse(op_output.output);
            const schema_output_names = Object.keys(op.output_schemas[op_output.branch]!.shape);
            const global_output_names = schema_output_names.map(n => op.output_map?.[n] ?? n);
            for (let i = 0; i < schema_output_names.length; i++) {
                const schema_name = schema_output_names[i]!;
                const global_name = global_output_names[i]!;
                const entry = resolves.get(global_name);
                if (!entry)
                    throw new Error(`Resolve not found for output "${global_name}" in node "${name}"`);
                entry.resolve(parsed_output[schema_name]);
            }
            this.log(`[${name}] END — outputs produced: [${global_output_names.join(", ")}] (branch: ${op_output.branch})`);
        } catch (err) {
            // On failure, reject all of this operator's output promises so
            // downstream operators don't hang indefinitely.
            this.log(`[${name}] END — error, rejecting outputs: [${all_global_output_names.join(", ")}]`, err);
            for (const global_name of all_global_output_names) {
                resolves.get(global_name)?.reject(err);
            }
        }
    }
}