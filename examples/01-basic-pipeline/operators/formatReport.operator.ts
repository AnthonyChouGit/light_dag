import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

export const FormatReportOptionsSchema = z.object({
    title: z.string().default("Daily Report")
});
export type FormatReportOptions = z.infer<typeof FormatReportOptionsSchema>;

const FormatReportInputSchema = z.object({
    items: z.array(z.string())
});

const FormatReportOutputSchema = z.object({
    report: z.string()
});

export default async function formatReport(
    { inputs, options }: OperatorArgs
): Promise<OperatorOutput> {
    const { items } = inputs as { items: string[] };
    const { title } = options as FormatReportOptions;

    const report = [`# ${title}`, ...items.map((item, i) => `${i + 1}. ${item}`)].join("\n");
    return { branch: "default", output: { report } };
}

export class FormatReportOperator extends Operator {
    override name = "format_report";
    override input_schema = FormatReportInputSchema;
    override output_schemas = { default: FormatReportOutputSchema };
    override options_schema = FormatReportOptionsSchema;
    override exec = formatReport;
}

export class FormatReportOperatorThread extends Operator {
    override name = "format_report";
    override input_schema = FormatReportInputSchema;
    override output_schemas = { default: FormatReportOutputSchema };
    override options_schema = FormatReportOptionsSchema;
    override exec = import.meta.filename;
}

