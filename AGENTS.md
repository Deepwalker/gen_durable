# AGENTS.md

Instructions for AI agents working on gen_durable.

- **Conventions** (tests in the devcontainer, performance contract, versioned migrations,
  0.2.x versioning, docs-in-the-same-change, the `ISSUES.md` ledger): [`CLAUDE.md`](CLAUDE.md).
- **Orientation**: [`ARCHITECTURE.md`](ARCHITECTURE.md). Read it before grepping.
- **Formal models**: [`formal/AGENTS.md`](formal/AGENTS.md). The Quint/TLC/Apalache models
  of the concurrency protocol, when a change must update them (claim, ownership guard,
  admission/credit, flush, inline run-ahead, reaper/reclaim/attempts, signals), and how to run
  them (`make formal`). A protocol change is not done until `make formal` is green.
