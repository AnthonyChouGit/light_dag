import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

export const TruncateItemsOptionsSchema = z.object({
    max_items: z.number().int().positive().default(3)
});
export type TruncateItemsOptions = z.infer<typeof TruncateItemsOptionsSchema>;

const TruncateItemsInputSchema = z.object({
    items: z.array(z.string())
});

const TruncateItemsOutputSchema = z.object({
    truncated_items: z.array(z.string())
});

export default async function truncateItems(
    { inputs, options }: OperatorArgs
): Promise<OperatorOutput> {
    const { items } = inputs as { items: string[] };
    const { max_items } = options as TruncateItemsOptions;

    return {
        branch: "default",
        output: { truncated_items: items.slice(0, max_items) }
    };
}

export class TruncateItemsOperator extends Operator {
    override name = "truncate_items";
    override input_schema = TruncateItemsInputSchema;
    override output_schemas = { default: TruncateItemsOutputSchema };
    override options_schema = TruncateItemsOptionsSchema;
    override exec = truncateItems;
}

