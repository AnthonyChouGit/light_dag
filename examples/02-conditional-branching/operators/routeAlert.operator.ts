import * as z from "zod";
import { Operator, type OperatorArgs, type OperatorOutput } from "../../../src/index.js";

const RouteAlertInputSchema = z.object({
    severity: z.enum(["high", "low"]),
    message: z.string().nonempty()
});
type RouteAlertInput = z.infer<typeof RouteAlertInputSchema>;

const UrgentOutputSchema = z.object({
    urgent_msg: z.string()
});

const NormalOutputSchema = z.object({
    normal_msg: z.string()
});

export default async function routeAlert({ inputs }: OperatorArgs): Promise<OperatorOutput> {
    const { severity, message } = inputs as RouteAlertInput;

    if (severity === "high") {
        return { branch: "urgent", output: { urgent_msg: `[URGENT] ${message}` } };
    }
    return { branch: "normal", output: { normal_msg: `[INFO] ${message}` } };
}

export class RouteAlertOperator extends Operator {
    override name = "route_alert";
    override input_schema = RouteAlertInputSchema;
    override output_schemas = {
        urgent: UrgentOutputSchema,
        normal: NormalOutputSchema
    };
    override exec = routeAlert;
}

