import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

const LongTaskInputSchema = z.object({
    job_id: z.string()
});

const LongTaskOutputSchema = z.object({
    long_result: z.string()
});

export default async function longTask({ inputs, signal }: OperatorArgs): Promise<OperatorOutput> {
    const { job_id } = inputs as { job_id: string };
    console.log(`[long_task] Started job "${job_id}"`);

    await new Promise<void>((resolve, reject) => {
        if (signal?.aborted) {
            return reject(signal.reason);
        }

        const timer = setTimeout(() => {
            console.log(`[long_task] Completed job "${job_id}"`);
            resolve();
        }, 2000);

        signal?.addEventListener("abort", () => {
            clearTimeout(timer);
            console.log(`[long_task] Received abort signal; cleaning up job "${job_id}"`);
            reject(signal.reason);
        }, { once: true });
    });

    return { branch: "default", output: { long_result: `ok:${job_id}` } };
}

export class LongTaskOperator extends Operator {
    override name = "long_task";
    override input_schema = LongTaskInputSchema;
    override output_schemas = { default: LongTaskOutputSchema };
    override exec = longTask;
}

