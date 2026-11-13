import { branching_dag } from "./branching.dag.js";

async function main() {
    const sharedOptions = {
        sms_label: "SMS",
        email_label: "EMAIL",
        log_label: "LOG"
    };

    console.log("=== Case 1: High Severity (urgent branch) ===");
    const highRes = await branching_dag.run(
        { severity: "high", message: "Database CPU at 98%" },
        ["urgent_done"],
        undefined,
        sharedOptions
    );
    console.log("Output:", highRes);

    console.log("\n=== Case 2: Low Severity (normal branch) ===");
    const lowRes = await branching_dag.run(
        { severity: "low", message: "Scheduled backup completed" },
        ["normal_done"],
        undefined,
        sharedOptions
    );
    console.log("Output:", lowRes);
}

main().catch(console.error);

