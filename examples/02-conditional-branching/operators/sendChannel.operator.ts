import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

const SendChannelInputSchema = z.object({
    payload: z.string()
});

const SendChannelOutputSchema = z.object({
    delivered: z.boolean()
});

const SendChannelOptionsSchema = z.object({
    channel_label: z.string().default("CONSOLE")
});

export default async function sendChannel({ inputs, options }: OperatorArgs): Promise<OperatorOutput> {
    const { payload } = inputs as { payload: string };
    const { channel_label } = options as z.infer<typeof SendChannelOptionsSchema>;

    console.log(`[${channel_label}] Delivered: ${payload}`);
    return { branch: "default", output: { delivered: true } };
}

export class SendChannelOperator extends Operator {
    override name = "send_channel";
    override input_schema = SendChannelInputSchema;
    override output_schemas = { default: SendChannelOutputSchema };
    override options_schema = SendChannelOptionsSchema;
    override exec = sendChannel;
}

