# LightDAG

A lightweight reactive Directed Acyclic Graph (DAG) execution engine for TypeScript / Node.js.

## Features

- **Reactive Promise-driven execution**: Zero polling; downstream operators automatically wake up as soon as input promises resolve.
- **Contract-first architecture**: Strict input, output, options, and requires schema validation via [Zod](https://github.com/colinhacks/zod).
- **Flexible namespace mapping**: Decouple local operator schema names from global DAG task names via chainable `.mapInput()`, `.mapOutput()`, etc.
- **Worker pool support**: Seamless offloading of CPU/IO-intensive tasks to OS threads via [Piscina](https://github.com/piscinajs/piscina).

## Architecture Proposals

See [REFACTORING_PROPOSAL.md](./REFACTORING_PROPOSAL.md) for the detailed proposal on framework-level error handling, graceful lifecycle management, and active execution draining.
