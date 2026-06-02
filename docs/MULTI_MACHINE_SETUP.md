# Multi-Machine Setup

Running Baby's First MapReduce across several physical machines requires **no code
changes** — workers are separate Erlang nodes that connect to the master by name over
the LAN. This is the complete setup guide: cluster layout, prerequisites, network and
cookie configuration, launch order, verification, and troubleshooting — followed by a
reference on where data lives and the filesystem options.

> A condensed quick-reference (launch commands + env-var table) also lives in
> [`config/multi_machine_example.md`](../config/multi_machine_example.md).

---

## Cluster layout (worked example)

This guide uses a concrete 3-MacBook layout; adjust the counts to your hardware:

| Machine | Runs | Node name(s) |
|---|---|---|
| `macbook1.local` | master | `master@macbook1.local` |
| `macbook2.local` | 2 workers | `worker1@macbook2.local`, `worker2@macbook2.local` |
| `macbook3.local` | 2 workers | `worker3@macbook3.local`, `worker4@macbook3.local` |

That's 4 workers, so set `MR_MIN_WORKERS=4` — the master waits for that many to register
before starting the job. (You can also run a worker on the master machine itself; just
count it toward the total.)

## 1. Prerequisites

- **Same build everywhere.** Check out the **same git commit** on all machines and build
  it (`mix deps.get && mix compile`). A divergent build risks message-decoding mismatches
  between nodes.
- **Same Elixir/OTP — pinned via `.tool-versions`.** The repo pins the exact toolchain
  (Erlang/OTP 29.0, Elixir 1.20.0-rc.5) in `/.tool-versions`. The apps require
  `elixir ~> 1.20-rc`, so a plain `brew install elixir` (older stable) **won't compile** —
  reproduce the toolchain with [`asdf`](https://asdf-vm.com) instead:
  ```bash
  brew install asdf autoconf openssl@3 wxwidgets   # asdf + Erlang build deps
  asdf plugin add erlang && asdf plugin add elixir
  asdf install                                      # reads .tool-versions in the repo dir
  ```
  `asdf install` builds OTP 29 from source (a one-time ~10–20 min compile per machine). If
  asdf can't find the exact Elixir build string, list options with
  `asdf list-all elixir | grep 1.20` and adjust the `elixir` line in `.tool-versions`.
- **Input must be reachable by each worker.** Map workers read input files from the
  `--input` path locally, so that path must exist on **every** machine running a worker
  (copy `sample-data/` to each, or use an identical path). Only *final output* is
  centralized on the master; input is read locally.

## 2. Network

Workers find the master by node name, which resolves to a hostname. On a LAN, macOS
`.local` (mDNS/Bonjour) names work out of the box:

- Verify resolution **before** launching: `ping macbook1.local` from each worker machine.
  If that fails, distribution will fail too.
- Erlang distribution needs **epmd (TCP 4369)** plus the dynamic distribution port range
  reachable between machines. If a firewall is on, allow `beam.smp`/`epmd` (or open those
  ports), or `Node.connect` will hang/fail.

## 3. Cookie and node names — the two things that must match

These are the two values that, if wrong, produce a cluster that **silently never forms**:

- **`MR_COOKIE`** — the Erlang distribution shared secret. Every node (master and all
  workers) must use the **same** value. A mismatch makes `Node.connect` return `false`;
  the worker retries with backoff and never registers.
- **`MR_MASTER_NODE`** — the master's node name, e.g. `master@macbook1.local`. The master
  boots under this name; every worker uses it as the connect target. Must be **identical**
  on the master and all workers, and its host part must be resolvable (step 2).

## 4. Output and filesystem

Use the default — **each reduce worker writes its output to its own local disk** (no
shared filesystem). Final output lands in `MR_WORKER_OUTPUT_DIR` (default `output/`) **on
each reduce worker's machine**, so on a multi-machine run it's scattered across the
workers. See [Where data lives](#where-data-lives) below for the full model and the
optional shared-mount alternative. Intermediate data always stays local to each worker —
never share it.

## 5. Launch

Start **workers first** (they retry the connection with backoff until the master is up, so
order is forgiving), then the master.

On **each worker machine**, once per worker, each with a unique `--name`:

```bash
MR_MASTER_NODE=master@macbook1.local MR_COOKIE=secret MR_COORDS=80.0,20.0 \
  mix mr.worker --name worker1@macbook2.local
```

Repeat for `worker2@macbook2.local`, `worker3@macbook3.local`, … Give each a distinct
`MR_COORDS` (decimals, e.g. `80.0,20.0`) if you want the locality simulation to show
separation between workers; omit it to self-assign random coordinates.

On **macbook1** (master):

```bash
MR_MASTER_NODE=master@macbook1.local MR_COOKIE=secret MR_MIN_WORKERS=4 \
  mix mr.start --distributed --input sample-data/ --reducers 4
```

`--distributed` makes the master **wait for `MR_MIN_WORKERS` registrations** instead of
spawning local workers. Other flags: `--task` (`word_count` or `distributed_grep`),
`--reducers N`, `--input PATH`.

## 6. Verify it worked

Watch the master log for:

- `[master] worker_registered | node=worker1@macbook2.local …` — one per worker; the job
  starts once `MR_MIN_WORKERS` have registered.
- `[master] task_assigned | type=map …`, then `type=reduce …` as work flows.
- `=== MapReduce Job Complete ===` with a duration.

Final results are written to `MR_WORKER_OUTPUT_DIR` (default `output/`) **on each reduce
worker's machine** (macbook2/macbook3 here), as `bucket-0.txt`, `bucket-1.txt`, … one per
reducer — so they're spread across the workers. Gather them with `scp`/`rsync`, or point
`MR_WORKER_OUTPUT_DIR` at a shared mount to collect them in one place.

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Workers log `register attempt N failed, retry in …`; master shows no `worker_registered` | **Cookie mismatch** (`Node.connect` → `false`) | Use an identical `MR_COOKIE` on every node. |
| Same, but the cookie is correct | Master node name wrong/unreachable | `MR_MASTER_NODE` must be identical everywhere and the host must `ping`. |
| Worker exits: `cannot connect: distribution not started` | `net_kernel` not up (`Node.connect` → `:ignored`) | Launch via `mix mr.worker` (it starts distribution); don't use plain `mix run`. |
| `Protocol 'inet_tcp': the name worker1@… seems to be in use` | A previous BEAM with that node name is **still registered in epmd** | List with `epmd -names`; kill the orphaned `beam.smp` (or use a different `--name`). |
| Nodes can't see each other at all | **Firewall** blocking epmd (4369) or the distribution port range | Allow `epmd`/`beam.smp` through the firewall on each machine. |
| Job never starts; master sits idle | Fewer workers registered than `MR_MIN_WORKERS` | Launch enough workers, or lower `MR_MIN_WORKERS`. |
| Map task crashes with a "no such file" error | `--input` path doesn't exist on a worker machine | Ensure the input path exists on every worker (copy the data). |

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

## Final output: where it lands

### Default — each reduce worker writes to its own local disk

Reduce workers write their final output straight to **their own local disk**:

1. `ReduceTask.execute` fetches the intermediate buckets, runs `reduce/2`, and writes the
   result to `<MR_WORKER_OUTPUT_DIR>/bucket-<n>.txt` on the machine it's running on.
2. It then casts `{:reduce_done, …}` to the master so completion can be tracked — the
   master never sees the output bytes.

- Configured by `MR_WORKER_OUTPUT_DIR` (env) → `:mr_worker, :output_base_dir`, **default `output`**.
- Filenames are deterministic per reduce bucket (`bucket-<n>.txt`), so a re-run after a
  worker failure overwrites rather than duplicates.

**What this means on a multi-machine run:** the output is **scattered** — each reduce
worker leaves its `bucket-<n>.txt` on its own machine. There's no shared filesystem, so
to assemble the full result set you either gather the files yourself (`scp`/`rsync` from
each worker) or use the shared-mount option below. On a **single-machine** run the local
workers share the master's working directory, so everything simply appears in `output/`
with no extra step.

**Relation to the paper:** the original MapReduce has reduce workers write final output to
GFS — a *globally readable* distributed filesystem — so "written locally by the reducer"
and "available everywhere" are the same thing. We write locally but have no shared FS, so
we get the reducer-writes-its-own-output half without the global-visibility half. The
shared-mount option restores the second half.

### Shared output directory (optional, GFS-faithful)

To collect all output in one place — and get closer to the paper's GFS model — point
`MR_WORKER_OUTPUT_DIR` at a **shared mount** (NFS, `/Volumes/shared`, etc.) that every
worker machine has mounted at the same path. Reducers then write their buckets straight
into the shared directory; no code change, just the env var.

- **Pro:** all `bucket-<n>.txt` land in one directory; closest to the paper.
- **Con:** real setup cost — every machine needs the mount, with consistent paths and
  write permissions; you inherit NFS's failure and consistency quirks.

(Note: this only concerns *final output*. Intermediate map data stays node-local
regardless — see above.)

---

## Configuration summary

| Concern | Env var | Application key | Default | Shared FS? |
|---|---|---|---|---|
| Intermediate (map) temp dir | `MR_TEMP_DIR` | `:mr_worker, :temp_base_dir` | `tmp` | **No** |
| Final output dir | `MR_WORKER_OUTPUT_DIR` | `:mr_worker, :output_base_dir` | `output` | Only if pointed at a shared mount |

For a typical multi-machine run, leave both at their defaults: temp and output both land
locally on each worker (output as `bucket-<n>.txt` under `output/`). Gather the output
afterward with `scp`/`rsync`, or set `MR_WORKER_OUTPUT_DIR` to a shared mount to
centralize it.
