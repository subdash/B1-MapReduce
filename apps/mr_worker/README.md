# mr_worker

The **worker node** of Baby's First MapReduce. Part of the umbrella project — see the [top-level README](../../README.md).

A worker (`MrWorker.Worker`) registers with the master and runs whatever it's assigned:

- `MrWorker.MapTask` — reads an input file, applies the task's `map/2` (and optional `combine/2`), and writes partitioned intermediate buckets to **local disk**.
- `MrWorker.ReduceTask` — fetches the relevant buckets from map workers via RPC, applies `reduce/2`, and returns the result for the master to collect.
- `MrWorker.FileServer` — serves this worker's local intermediate files to reduce workers on request (non-blocking, so concurrent fetches don't serialize).
- `MrWorker.RPC` — the cross-node call wrapper that also simulates locality latency based on node coordinates.

Built-in example tasks live under `MrWorker.Tasks` (`WordCount`, `DistributedGrep`). A worker node is launched with the `mix mr.worker` task; see [`docs/MULTI_MACHINE_SETUP.md`](../../docs/MULTI_MACHINE_SETUP.md).
