# mr_protocol

**Shared types and contracts** for Baby's First MapReduce, depended on by both `mr_master` and `mr_worker`. Part of the umbrella project — see the [top-level README](../../README.md).

It holds the cross-node data structures and the pluggable task interface, so the master and workers agree on the messages they exchange:

- `MrProtocol.Task` — the behaviour every job implements (`map/2`, `reduce/2`, optional `combine/2`).
- `MrProtocol.MapTask`, `MrProtocol.ReduceTask`, `MrProtocol.WorkerInfo` — the structs passed between master and workers.
- `MrProtocol.Distance`, `MrProtocol.ParseCoords` — helpers for the fictional-coordinate locality simulation.

This app has no processes of its own — it's pure data and behaviour definitions.
