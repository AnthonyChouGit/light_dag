import { resilient_dag } from "./resilient.dag.js";

async function main() {
    // 1. Without setOnError: framework logs a built-in fallback error message
    console.log("--- Run 1: Default framework fallback ---");
    await resilient_dag.run({ job_id: "job-41" }, ["summary"]);

    // 2. With setOnError: one unified DAG-level handler; branch on node_name for operator-specific actions
    console.log("\n--- Run 2: Custom setOnError handler ---");
    resilient_dag.setOnError(async (err, node_name) => {
        if (node_name === "failing_task") {
            console.error(`[AlertHook] Upstream failure in "${node_name}":`, (err as Error)?.message ?? err);
        } else {
            console.error(`[AlertHook] Unexpected failure in "${node_name}":`, err);
        }
    });

    const resource = {
        closed: false,
        async close() {
            this.closed = true;
            console.log("[Cleanup] External resource pool closed safely.");
        }
    };

    try {
        const res = await resilient_dag.run({ job_id: "job-42" }, ["summary"]);
        if (res) {
            console.log("Result:", res.summary);
        }
    } finally {
        // Executed only after all in-flight operators (including long_task) have exited
        await resource.close();
    }
}

main().catch(console.error);


