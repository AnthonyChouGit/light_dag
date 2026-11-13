import * as z from "zod";
import { LightDag, Operator, type OperatorArgs, type OperatorOutput } from "../../src/index.js";
import { LongTaskOperator } from "./operators/longTask.operator.js";
import { FailingTaskOperator } from "./operators/failingTask.operator.js";

class CombineOperator extends Operator {
    override name = "combine";
    override input_schema = z.object({
        fast_result: z.string(),
        long_result: z.string()
    });
    override output_schemas = {
        default: z.object({ summary: z.string() })
    };
    override exec = async ({ inputs }: OperatorArgs): Promise<OperatorOutput> => ({
        branch: "default",
        output: { summary: `${inputs.fast_result} + ${inputs.long_result}` }
    });
}

export const resilient_dag = new LightDag([
    new FailingTaskOperator(),
    new LongTaskOperator(),
    new CombineOperator()
]);

