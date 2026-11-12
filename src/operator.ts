import { z } from "zod";

/** Schema for the argument bundle passed to an operator's exec function. */
export const OperatorArgsSchema = z.object({
    inputs: z.record(z.string(), z.unknown()).default({}),
    requires: z.record(z.string(), z.unknown()).default({}),
    options: z.record(z.string(), z.unknown()).default({})
});
export type OperatorArgs = z.infer<typeof OperatorArgsSchema>;

/** Schema for the value returned by an operator's exec function. */
export const OperatorOutputSchema = z.object({
    /** Which output branch was taken (selects the schema from output_schemas). */
    branch: z.string().nonempty(),
    output: z.record(z.string(), z.unknown())
});
export type OperatorOutput = z.infer<typeof OperatorOutputSchema>;

/**
 * Base class for a DAG operator.
 *
 * Each operator defines Zod schemas for its inputs, outputs, requires (context),
 * and options. The schema keys are "schema names" — local to this operator.
 * The DAG itself uses "global names" to wire operators together.
 *
 * Name maps (input_map, output_map, requires_map, options_map) translate between
 * the two: { schema_name → global_name }. When no mapping is provided for a key,
 * the schema name is used as the global name.
 */
export abstract class Operator {
    name: string = "operator";

    /** Zod schema defining the operator's DAG inputs (wired via promises). */
    abstract input_schema: z.ZodObject;
    /** Zod schemas for each output branch, keyed by branch name. */
    abstract output_schemas: Record<string, z.ZodObject>;
    /** Zod schema for context values (e.g. API keys, config). Defaults to empty. */
    requires_schema: z.ZodObject = z.object({});
    /** Zod schema for runtime options. Defaults to empty. */
    options_schema: z.ZodObject = z.object({});

    /** Maps schema input names → global DAG task names. */
    input_map?: Record<string, string>;
    /** Maps schema output names → global DAG task names. */
    output_map?: Record<string, string>;
    /** Maps schema requires names → global context keys. */
    requires_map?: Record<string, string>;
    /** Maps schema options names → global options keys. */
    options_map?: Record<string, string>;

    /** The operator's execution function, or a filepath to a worker script. */
    abstract exec:
        | ((args: OperatorArgs) => Promise<OperatorOutput>)
        | string;

    /** Alias a schema input name to a different global DAG task name. Chainable. */
    public mapInput(schema_name: string, global_name: string): Operator {
        if (!Object.hasOwn(this.input_schema.shape, schema_name))
            throw new Error(`Schema input name ${schema_name} does not exist in operator ${this.name}`);
        if (!this.input_map)
            this.input_map = {};
        this.input_map[schema_name] = global_name;
        return this;
    }

    /** Alias a schema output name to a different global DAG task name. Chainable. */
    public mapOutput(schema_name: string, global_name: string): Operator {
        const exists = Object.values(this.output_schemas).some(
            (schema) => Object.hasOwn(schema.shape, schema_name)
        );
        if (!exists)
            throw new Error(`Schema output name ${schema_name} does not exist in operator ${this.name}`);
        if (!this.output_map)
            this.output_map = {};
        this.output_map[schema_name] = global_name;
        return this;
    }

    /** Alias a schema requires name to a different global context key. Chainable. */
    public mapRequires(schema_name: string, global_name: string): Operator {
        if (!Object.hasOwn(this.requires_schema.shape, schema_name))
            throw new Error(`Schema requires name ${schema_name} does not exist in operator ${this.name}`);
        if (!this.requires_map)
            this.requires_map = {};
        this.requires_map[schema_name] = global_name;
        return this;
    }

    /** Alias a schema options name to a different global options key. Chainable. */
    public mapOptions(schema_name: string, global_name: string): Operator {
        if (!Object.hasOwn(this.options_schema.shape, schema_name))
            throw new Error(`Schema options name ${schema_name} does not exist in operator ${this.name}`);
        if (!this.options_map)
            this.options_map = {};
        this.options_map[schema_name] = global_name;
        return this;
    }

    constructor(name?: string, input_map?: Record<string, string>, output_map?: Record<string, string>, requires_map?: Record<string, string>, options_map?: Record<string, string>) {
        if (name)
            this.name = name;
        if (input_map)
            this.input_map = input_map;
        if (output_map)
            this.output_map = output_map;
        if (requires_map)
            this.requires_map = requires_map;
        if (options_map)
            this.options_map = options_map;
    }

    public setName(name: string): Operator {
        this.name = name;
        return this;
    }


}