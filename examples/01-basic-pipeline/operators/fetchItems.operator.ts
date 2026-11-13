import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

export const FetchItemsOptionsSchema = z.object({
    prefix: z.string().default("item")
});
export type FetchItemsOptions = z.infer<typeof FetchItemsOptionsSchema>;

const FetchItemsInputSchema = z.object({
    topics: z.array(z.string()).nonempty()
});
type FetchItemsInput = z.infer<typeof FetchItemsInputSchema>;

const FetchItemsOutputSchema = z.object({
    fetched_items: z.array(z.string())
});

const FetchItemsRequiresSchema = z.object({
    source_name: z.string()
});

export default async function fetchItems(
    { inputs, requires, options, signal }: OperatorArgs
): Promise<OperatorOutput> {
    const { topics } = inputs as FetchItemsInput;
    const { source_name } = requires as z.infer<typeof FetchItemsRequiresSchema>;
    const { prefix } = options as FetchItemsOptions;

    const fetched_items: string[] = [];
    for (const topic of topics) {
        signal?.throwIfAborted();
        fetched_items.push(`[${source_name}] ${prefix}:${topic}-1`, `[${source_name}] ${prefix}:${topic}-2`);
    }

    return { branch: "default", output: { fetched_items } };
}

export class FetchItemsOperator extends Operator {
    override name = "fetch_items";
    override input_schema = FetchItemsInputSchema;
    override output_schemas = { default: FetchItemsOutputSchema };
    override requires_schema = FetchItemsRequiresSchema;
    override options_schema = FetchItemsOptionsSchema;
    override exec = fetchItems;
}

