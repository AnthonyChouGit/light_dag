import { type Piscina } from "piscina";
import { type Operator, type OperatorArgs, OperatorOutputSchema } from "./operator.js";

/** Internal wrapper preserving the failing operator's name and original thrown error across promise chains. */
class DagNodeError extends Error {
    constructor(
        public readonly node_name: string,
        public override readonly cause: unknown
    ) {
        const msg = cause instanceof Error ? cause.message : String(cause);
        super(`[LightDag] Operator "${node_name}" failed: ${msg}`);
        this.name = "DagNodeError";
    }
}

/**
 * Unified DAG-level error handler invoked when any operator's exec() throws an error.
 * Register a single handler for the entire DAG; inspect `node_name` (e.g. via `if` / `switch`)
 * only when you need different recovery logic for specific operators.
 * @param error     The original error thrown inside exec() (e.g. DB error, network error, etc.).
 * @param node_name The unique name of the operator that failed.
 */
export type ErrorHandlerHook = (error: unknown, node_name: string) => Promise<void> | void;

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
    private error_handler?: ErrorHandlerHook | undefined;

    constructor(operators: Operator[], debug?: boolean, timeout?: number, error_handler?: ErrorHandlerHook) {
        this.debug = debug ?? false;
        this.timeout = timeout;
        this.error_handler = error_handler;
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

    public setOnError(handler: ErrorHandlerHook): void {
        this.error_handler = handler;
    }

    private log(...args: unknown[]): void {
        if (this.debug)
            console.log("[LightDag]", ...args);
    }

    /**
     * Validate DAG wiring and detect dependency cycles (invoked when debug is enabled).
     * Checks for: input/output collisions, duplicate producers, dangling tasks, and cycles.
     */
    private validateGraph(inputs: Record<string, unknown>, outputs: string[]): void {
        const consumed = new Set<string>();
        const producer_of = new Map<string, string>();
        const provided_inputs = new Set(Object.keys(inputs));
        const requested_outputs = new Set(outputs);

        for (const op of this.operators.values()) {
            for (const name of Object.keys(op.input_schema.shape))
                consumed.add(op.input_map?.[name] ?? name);

            // Deduplicate across branches so a single operator producing the same
            // task on multiple branches isn't flagged as a duplicate producer.
            const op_outputs = new Set<string>();
            for (const branchSchema of Object.values(op.output_schemas))
                for (const name of Object.keys(branchSchema.shape))
                    op_outputs.add(op.output_map?.[name] ?? name);

            for (const name of op_outputs) {
                if (provided_inputs.has(name))
                    console.warn(`[LightDag] Warning: task "${name}" is both provided as input and produced by operator "${op.name}"`);
                if (producer_of.has(name))
                    console.warn(`[LightDag] Warning: task "${name}" is produced by multiple operators ("${producer_of.get(name)}" and "${op.name}")`);
                producer_of.set(name, op.name);
            }
        }

        for (const name of producer_of.keys()) {
            if (!consumed.has(name) && !requested_outputs.has(name))
                console.warn(`[LightDag] Warning: task "${name}" is produced but never consumed by any operator or requested as output`);
        }
        for (const name of consumed) {
            if (!producer_of.has(name) && !provided_inputs.has(name))
                console.warn(`[LightDag] Warning: task "${name}" is consumed but never produced by any operator or provided as input`);
        }
        for (const name of provided_inputs) {
            if (!consumed.has(name) && !requested_outputs.has(name))
                console.warn(`[LightDag] Warning: input "${name}" is provided but never consumed by any operator or requested as output`);
        }

        // Cycle detection via 3-state DFS (1 = visiting on current path, 2 = fully explored).
        // Walking upstream from each operator's inputs via producer_of detects any directed cycle.
        const state = new Map<string, 1 | 2>();
        const visit = (op_name: string): void => {
            const s = state.get(op_name);
            if (s === 1)
                throw new Error(`[LightDag] Cycle detected involving operator "${op_name}"`);
            if (s === 2)
                return;
            state.set(op_name, 1);
            const op = this.operators.get(op_name)!;
            for (const name of Object.keys(op.input_schema.shape)) {
                const upstream = producer_of.get(op.input_map?.[name] ?? name);
                if (upstream)
                    visit(upstream);
            }
            state.set(op_name, 2);
        };
        for (const op_name of this.operators.keys())
            visit(op_name);
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
    ): Promise<Record<string, unknown> | undefined> {
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
        // Track only operators currently inside exec() so finally can drain them cleanly
        // without waiting on unchosen conditional branches suspended at await inputs.
        const active_executions = new Set<Promise<unknown>>();
        const abort_controller = new AbortController();
        let timeout_id: NodeJS.Timeout | undefined;
        if (this.timeout)
            timeout_id = setTimeout(() => {
                const err = new Error(`[LightDag] Error: Execution timeout: ${this.timeout}ms`);
                abort_controller.abort(err);
                for (const { reject } of resolves.values())
                    reject(err);
            }, this.timeout);
        try {
            if (this.debug)
                this.validateGraph(inputs, outputs);

            // Kick off all operators concurrently — they'll block on their input promises.
            for (const op_name of this.operators.keys())
                this.runNode(op_name, tasks, resolves, active_executions, abort_controller, context, options, pool);
            // Resolve the initial input tasks to unblock the first wave of operators.
            for (const [input_name, input_value] of Object.entries(inputs)) {
                const entry = resolves.get(input_name);
                if (!entry)
                    throw new Error(`[LightDag] Input "${input_name}" not found in the DAG`);

                entry.resolve(input_value);
            }
            // Wait for the requested output tasks to resolve and collect results.
            const output_promises = outputs.map((output_name) => {
                const task = tasks.get(output_name);
                if (!task)
                    throw new Error(`[LightDag] Requested output "${output_name}" does not exist in the DAG`);
                return task;
            });
            const resolved_outputs = await Promise.all(output_promises);
            const output_values: Record<string, unknown> = {};
            for (let i = 0; i < outputs.length; i++) {
                output_values[outputs[i]!] = resolved_outputs[i];
            }
            this.log("run() completed");
            return output_values;
        } catch (err) {
            // Broadcast cancellation to all in-flight operators.
            abort_controller.abort(err);

            if (err instanceof DagNodeError) {
                // User-defined operator exec() error: delegate to setOnError or use framework fallback.
                if (this.error_handler) {
                    try {
                        await this.error_handler(err.cause, err.node_name);
                    } catch (hook_err) {
                        console.error("[LightDag] Error in onError handler:", hook_err);
                    }
                } else {
                    console.error(`[LightDag] Unhandled error in operator "${err.node_name}":`, err.cause);
                }
            } else {
                // Framework-level error (wiring, schema validation, cycle detection, or timeout).
                console.error("[LightDag] Framework error:", err);
            }
            return undefined;
        } finally {
            if (timeout_id)
                clearTimeout(timeout_id);
            // Drain all in-flight exec() calls before returning so outer cleanup (e.g.
            // closing DB connections or worker pools) never races with active operators.
            if (active_executions.size > 0)
                await Promise.allSettled(active_executions);
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
        active_executions: Set<Promise<unknown>>,
        abort_controller: AbortController,
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
            // Skip launching exec() if the DAG was already aborted while awaiting inputs.
            if (abort_controller.signal.aborted)
                return;

            const inputs: Record<string, unknown> = {};
            for (let i = 0; i < schema_input_names.length; i++) {
                inputs[schema_input_names[i]!] = input_results[i];
            }
            this.log(`[${name}] START — inputs resolved: [${global_input_names.join(", ")}]`);
            const parsed_inputs = op.input_schema.parse(inputs);

            if (typeof op.exec === "string" && !pool)
                throw new Error(`Worker pool not initialized for node ${name}`);

            const op_input_arg: OperatorArgs = {
                inputs: parsed_inputs,
                requires: parsed_requires,
                options: parsed_options,
                signal: abort_controller.signal
            };
            // Worker threads cannot structured-clone AbortSignal in the payload; pass it via Piscina RunOptions instead.
            const exec_promise = typeof op.exec === "string"
                ? pool!.run(
                    { inputs: parsed_inputs, requires: parsed_requires, options: parsed_options },
                    { filename: op.exec, signal: abort_controller.signal }
                )
                : op.exec(op_input_arg);

            active_executions.add(exec_promise);
            let raw_output: unknown;
            try {
                raw_output = await exec_promise;
            } catch (exec_err) {
                // If the DAG was already aborted (e.g. by a global timeout or another operator failing first),
                // this operator's rejection is a side-effect of cooperative cancellation (e.g. fetch/Piscina
                // throwing AbortError). Re-throw the original abort reason so we don't misattribute a timeout
                // or another node's failure as a new DagNodeError from this operator.
                if (abort_controller.signal.aborted)
                    throw abort_controller.signal.reason ?? exec_err;
                // Wrap genuine user exec() failures so run() routes them to setOnError instead of framework error logging.
                throw new DagNodeError(name, exec_err);
            } finally {
                active_executions.delete(exec_promise);
            }

            const op_output = OperatorOutputSchema.parse(raw_output);
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
            // Preserve DagNodeError (user exec failure) or upstream framework Error;
            // attach operator context only for local framework/schema errors.
            const propagated_err = (err instanceof DagNodeError || (err instanceof Error && err.message.startsWith("[LightDag]")))
                ? err
                : new Error(`[LightDag] Operator "${name}": ${err instanceof Error ? err.message : String(err)}`, { cause: err });
            this.log(`[${name}] END — error, rejecting outputs: [${all_global_output_names.join(", ")}]`, propagated_err);
            for (const global_name of all_global_output_names) {
                resolves.get(global_name)?.reject(propagated_err);
            }
        }
    }
}