import { DagNodeError } from "../../src/index.js";
import { resilient_dag } from "./resilient.dag.js";

async function main() {
    // Register centralized error alerting
    resilient_dag.setOnError(async (err: DagNodeError) => {
        console.error(`[AlertHook] Operator "${err.node_name}" failed:`, (err.cause as Error)?.message ?? err.cause);
    });

    const resource = {
        closed: false,
        async close() {
            this.closed = true;
            console.log("[Cleanup] External resource pool closed safely.");
        }
    };

    try {
        await resilient_dag.run({ job_id: "job-42" }, ["summary"]);
    } catch (err) {
        if (err instanceof DagNodeError) {
            console.error(`[Main] Caught DagNodeError from "${err.node_name}"`);
        } else {
            console.error("[Main] Unexpected error:", err);
        }
    } finally {
        // Executed only after all in-flight operators (including long_task) have exited
        await resource.close();
    }
}

main().catch(console.error);

