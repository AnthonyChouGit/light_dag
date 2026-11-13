import { LightDag } from "../../src/index.js";
import { FetchItemsOperator } from "./operators/fetchItems.operator.js";
import { TruncateItemsOperator } from "./operators/truncateItems.operator.js";
import { FormatReportOperator } from "./operators/formatReport.operator.js";

const fetch_op = new FetchItemsOperator();

// Reuse TruncateItemsOperator twice by giving each instance a unique name and remapping keys
const pre_truncate_op = new TruncateItemsOperator()
    .setName("pre_truncate")
    .mapInput("items", "fetched_items")
    .mapOutput("truncated_items", "pre_truncated_items")
    .mapOptions("max_items", "pre_max_items");

const post_truncate_op = new TruncateItemsOperator()
    .setName("post_truncate")
    .mapInput("items", "pre_truncated_items")
    .mapOutput("truncated_items", "final_items")
    .mapOptions("max_items", "post_max_items");

const format_op = new FormatReportOperator()
    .mapInput("items", "final_items");

export const basic_pipeline_dag = new LightDag([
    fetch_op,
    pre_truncate_op,
    post_truncate_op,
    format_op
]);

