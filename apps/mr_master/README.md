# mr_master

The **master node** of Baby's First MapReduce. Part of the umbrella project — see the [top-level README](../../README.md).

`MrMaster.Master` is the coordinator GenServer: it accepts job submissions, splits the input into map tasks, assigns map and reduce tasks to registered workers, tracks task and worker state, and drives the job through the mapping → reducing → done phases. It owns the fault-tolerance logic — requeuing tasks on `{:nodedown}`, launching speculative backup tasks for stragglers, and dropping stale completions from replaced/dead workers — and collects final reduce output on the master's disk via `MrMaster.OutputCollector`.

Locality-aware reduce scheduling lives in `MrMaster.Scheduler` (using each worker's fictional coordinates). The cluster is launched with the `mix mr.start` task; see the top-level README and [`config/README.md`](../../config/README.md) for run modes and options.
