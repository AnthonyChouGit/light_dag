import { LightDag } from "../../src/index.js";
import { RouteAlertOperator } from "./operators/routeAlert.operator.js";
import { SendChannelOperator } from "./operators/sendChannel.operator.js";
import { MergeStatusOperator } from "./operators/mergeStatus.operator.js";

const router = new RouteAlertOperator();

// Urgent branch: fans out to SMS + Email, then joins via MergeStatusOperator
const send_sms = new SendChannelOperator()
    .setName("send_sms")
    .mapInput("payload", "urgent_msg")
    .mapOutput("delivered", "sms_sent")
    .mapOptions("channel_label", "sms_label");

const send_email = new SendChannelOperator()
    .setName("send_email")
    .mapInput("payload", "urgent_msg")
    .mapOutput("delivered", "email_sent")
    .mapOptions("channel_label", "email_label");

const urgent_merge = new MergeStatusOperator("urgent_merge", ["sms_sent", "email_sent"], "urgent_done");

// Normal branch: logs to a single channel
const send_log = new SendChannelOperator()
    .setName("send_log")
    .mapInput("payload", "normal_msg")
    .mapOutput("delivered", "normal_done")
    .mapOptions("channel_label", "log_label");

export const branching_dag = new LightDag([
    router,
    send_sms,
    send_email,
    urgent_merge,
    send_log
]);

