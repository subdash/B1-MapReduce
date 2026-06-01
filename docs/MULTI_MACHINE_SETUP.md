# Multi-Machine Setup

Running Baby's First MapReduce across several physical machines requires **no code
changes** — workers are separate Erlang nodes that connect to the master by name over
the LAN. This document explains where data lives and the filesystem choices involved.

> **Scope:** This page currently covers **output collection and the filesystem model**
> (Task 4.3). The full step-by-step 3-MacBook walkthrough — network setup, cookies,
> launch order, and troubleshooting — is added in Task 5.2.

---

## Where data lives

A run produces two kinds of files in two very different places:

| Data | Written by | Lives on | Shared? |
|---|---|---|---|
| **Intermediate** (map output buckets) | map workers | each worker's **local** disk | **Never** — RPC-fetched on demand |
| **Final output** (reduce results) | reduce workers → master | the **master's** disk | n/a (single destination) |

The asymmetry is deliberate and traces directly to the paper (Dean & Ghemawat,
sections 3.1–3.4): intermediate data stays local and is pulled by reducers; final
output is collected in one place.

---

## Intermediate data: always worker-local (do not share)

Each map worker writes its `R` partition buckets to its own local disk:

```
<temp_base_dir>/<worker-node-name>/map-<task-id>-bucket-<r>.bin
```

- Configured by `MR_TEMP_DIR` (env) → `:mr_worker, :temp_base_dir`, **default `tmp`**.
- A reduce worker fetches each bucket it needs via RPC against the map worker that
  produced it: `MrWorker.RPC.call(node, MrWorker.FileServer, {:fetch, path}, …)`.

**Do not put `temp_base_dir` on a shared filesystem.** Sharing intermediate files
defeats the paper-faithful "local disk + RPC fetch" model and, more importantly,
breaks the fault-tolerance story: a dead map worker is *supposed* to make its
intermediate data unreachable, which is exactly what triggers the master to re-run
those map tasks. If the buckets sat on shared storage they'd remain readable after the
worker died, masking the failure the design is built to handle.

Pointing `MR_TEMP_DIR` at a **fast local path** (e.g. a local SSD scratch dir, or
`/tmp`) per machine is fine and encouraged — that's local, not shared.

---

## Final output: how it gets collected

### Option 1 — Master collects via RPC (default, recommended)

This is what the implementation does today (Task 4.2). Reduce workers do **not** write
output to their own disk. Instead:

1. `ReduceTask.execute` computes the result and returns `{filename, binary}` — a pure
   function with no filesystem or master dependency.
2. The worker pushes the bytes to the master over the throttled RPC path:
   `MrWorker.RPC.call(master_node, MrMaster.OutputCollector, {:write_output, filename, binary}, …)`.
3. `MrMaster.OutputCollector` (a GenServer on the master) writes the file into one
   directory on the master's disk.

- Configured by `MR_OUTPUT_DIR` (env) → `:mr_master, :output_base_dir`, **default `output`**.
- Filenames are deterministic per reduce bucket (`bucket-<n>.txt`), so a re-run after a
  worker failure overwrites rather than duplicates.

**Why this is the default:** zero filesystem setup. You can run across three machines
with nothing but the LAN — no NFS, no mounts, no permissions. All results land in
`output/` on the master, ready to inspect.

**Deliberate paper deviation:** the original MapReduce has reduce workers write final
output directly to the distributed filesystem (GFS); the master is only a coordinator,
never a data conduit. Here the master *is* the funnel for final output. That's a
conscious trade for simplicity on a small cluster. You'd outgrow it when final output
gets large or reducers numerous enough that serializing every write through one master
GenServer becomes the bottleneck — at which point Option 2 is the escape hatch.

### Option 2 — Shared output directory (optional, more GFS-faithful)

If you'd rather mirror the paper more closely and remove the master-as-funnel
bottleneck, point `output_base_dir` at a shared mount (NFS, `/Volumes/shared`, etc.)
and have reducers write there directly instead of pushing to the master.

- **Pro:** closer to the paper; no single-process write bottleneck; scales further.
- **Con:** real setup cost — every machine needs the mount, with consistent paths and
  write permissions; you inherit NFS's failure and consistency quirks.

This is **not wired up by default** — it's the documented alternative for when the
simple model stops being enough. (Note: this only concerns *final output*. Intermediate
data stays local regardless — see above.)

---

## Configuration summary

| Concern | Env var | Application key | Default | Shared FS? |
|---|---|---|---|---|
| Intermediate (map) temp dir | `MR_TEMP_DIR` | `:mr_worker, :temp_base_dir` | `tmp` | **No** |
| Final output dir | `MR_OUTPUT_DIR` | `:mr_master, :output_base_dir` | `output` | Only in Option 2 |

For a typical multi-machine run, leave both at their defaults: temp lands locally on
each worker, and output lands in `output/` on the master.
