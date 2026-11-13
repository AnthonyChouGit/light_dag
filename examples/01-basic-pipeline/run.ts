import { basic_pipeline_dag } from "./pipeline.dag.js";

async function main() {
    basic_pipeline_dag.setDebug(true);

    const result = await basic_pipeline_dag.run(
        { topics: ["tech", "science", "sports"] },
        ["report"],
        { source_name: "FeedA" },
        { prefix: "topic", pre_max_items: 4, post_max_items: 2, title: "Digest" }
    );

    console.log("\n--- Result ---");
    console.log(result.report);
}

main().catch(console.error);

