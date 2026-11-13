import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

const FailingTaskInputSchema = z.object({
    job_id: z.string()
});

const FailingTaskOutputSchema = z.object({
    fast_result: z.string()
});

export default async function failingTask({ inputs }: OperatorArgs): Promise<OperatorOutput> {
    const { job_id } = inputs as { job_id: string };
    await new Promise(r => setTimeout(r, 50));
    throw new Error(`Upstream connection refused for job "${job_id}"`);
}

export class FailingTaskOperator extends Operator {
    override name = "failing_task";
    override input_schema = FailingTaskInputSchema;
    override output_schemas = { default: FailingTaskOutputSchema };
    override exec = failingTask;
}

