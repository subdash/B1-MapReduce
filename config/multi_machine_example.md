# Multi-machine example: 3 MacBooks on a LAN

**Layout**

- `macbook1.local` — master + 1 worker
- `macbook2.local` — 2 workers
- `macbook3.local` — 2 workers   (so `min_workers = 5`)

## Launch

On **macbook1** (master):

```bash
MR_MASTER_NODE=master@macbook1.local MR_COOKIE=secret MR_MIN_WORKERS=5 \
  mix mr.start --distributed --input /path/to/input --reducers 4
```

On **each worker machine**, once per worker (example for macbook2):

```bash
MR_MASTER_NODE=master@macbook1.local MR_COOKIE=secret MR_COORDS=80.0,20.0 \
  mix mr.worker --name worker1@macbook2.local
```

Run this **once per worker**, each with a **unique `--name`** (e.g. `worker1@macbook2.local`, `worker2@macbook2.local`). Give each a distinct `MR_COORDS` if you want the locality simulation to show separation between workers; omit it to let each self-assign random coordinates.

## Environment variables

All values are read at boot in `config/runtime.exs`, so they can differ per launched node (no recompile needed).

| Variable | Read by | Default | Must match across nodes? | Purpose |
|---|---|---|---|---|
| `MR_COOKIE` | master + workers | `secret` | **Yes** | Erlang distribution cookie. A mismatch means `Node.connect` silently fails and the worker never registers. |
| `MR_MASTER_NODE` | master + workers | `master@127.0.0.1` | **Yes** | The master's node name. The master starts itself under this name; every worker uses it as the connect target. |
| `MR_MIN_WORKERS` | master | `4` | n/a | In `--distributed` mode, how many workers must register before the job starts. |
| `MR_OUTPUT_DIR` | master | `output` | n/a | Directory **on the master** where final reduce output is written (reducers push results to the master via RPC). |
| `MR_TEMP_DIR` | worker | `tmp` | n/a (per-machine) | Directory for a worker's local intermediate bucket files. Stays local — fetched by reducers via RPC, never shared. |
| `MR_COORDS` | worker | *(unset → random)* | n/a | Worker's fictional 2D coordinates `x,y` for the locality simulation. If unset, the worker self-assigns random coords. **Use decimals** (e.g. `80.0,20.0`). |

> `MR_START_MASTER` exists but is an internal compile-time switch (default `false`) selecting whether a build boots as master or worker. You don't set it in normal use: `mix mr.start` starts the master directly, and `mix mr.worker` / `mix run` start a worker because the default is `false`.

## Prerequisites & gotchas

- **Same build everywhere.** Clone the same commit and use the same Elixir/OTP versions on all three machines.
- **Hostname resolution.** Machines must resolve each other's `.local` names — verify with `ping macbook2.local` before launching.
- **Firewall / epmd.** Erlang distribution needs epmd (TCP 4369) plus the dynamic distribution port range reachable between machines, or `Node.connect` will fail.
- **Cookie + master node must be identical** on every node (see the table) — these are the two values that, if mismatched, produce a cluster that silently never forms.
- **Results live on the master.** With master-collected output, the final files appear in `MR_OUTPUT_DIR` on `macbook1`, not on the worker machines.
- **Worker count vs. `MR_MIN_WORKERS`.** The job won't start until that many workers register; make sure you actually launch at least that many.
