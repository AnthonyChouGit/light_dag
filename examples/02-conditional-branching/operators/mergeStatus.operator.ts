import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

const MergeStatusOutputSchema = z.object({
    fulfilled: z.boolean()
});

export default async function mergeStatus({ inputs }: OperatorArgs): Promise<OperatorOutput> {
    for (const [key, ok] of Object.entries(inputs as Record<string, boolean>)) {
        if (!ok) {
            throw new Error(`Upstream status "${key}" is false`);
        }
    }
    return { branch: "default", output: { fulfilled: true } };
}

export class MergeStatusOperator extends Operator {
    override input_schema: z.ZodObject;
    override output_schemas = { default: MergeStatusOutputSchema };
    override exec = mergeStatus;

    constructor(name: string, status_names: string[], fulfill_name?: string) {
        super(name);
        const shape: Record<string, z.ZodBoolean> = {};
        for (const key of status_names) {
            shape[key] = z.boolean();
        }
        this.input_schema = z.object(shape);
        if (fulfill_name) {
            this.mapOutput("fulfilled", fulfill_name);
        }
    }
}

