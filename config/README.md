# Configuration

How this project is configured, and how to start it in each of its two run modes.

## Config files

| File | When it runs | What lives here |
|---|---|---|
| `config.exs` | **Compile time** | Static defaults baked into the build (e.g. the `start_master` switch, dashboard/esbuild/tailwind config). Do **not** put per-machine values here — compile-time `System.get_env` reads get frozen into the build. |
| `runtime.exs` | **Boot time**, every start | Per-machine / per-launch values read from environment variables: cookie, master node name, worker coords, `min_workers`, output/temp dirs. This is where deployment config belongs. |
| `dev.exs` / `test.exs` / `prod.exs` | Compile time (imported by `config.exs`) | Environment-specific settings (mostly the dashboard endpoint). |

For the full list of environment variables read by `runtime.exs` — defaults, which process reads each, and which must match across nodes — see **[`multi_machine_example.md`](multi_machine_example.md)**.

## Run modes

The master is started with `mix mr.start`. It supports two modes.

### Local (single machine) — default

`mix mr.start` spawns worker nodes for you on localhost and waits for all of them to register before running the job. Each spawned worker gets random coordinates. This is the one-command development workflow.

```bash
mix mr.start --workers 4 --input sample-data/ --reducers 4
```

- `--workers N` — how many local workers to spawn (the job waits for all N).
- `--input` — input directory.
- `--reducers` — number of reduce partitions (R).

### Distributed (multi-machine)

`mix mr.start --distributed` does **not** spawn workers. It starts the master and waits until `MR_MIN_WORKERS` workers have registered — you start those workers yourself on the other machines with `mix mr.worker`. Once enough have registered, the job runs.

```bash
# on the master machine
MR_MASTER_NODE=master@macbook1.local MR_COOKIE=secret MR_MIN_WORKERS=5 \
  mix mr.start --distributed --input /path/to/input --reducers 4
```

Workers connect to the master by name and register themselves (the master never reaches out to them), so you only need to tell each worker the master's node name and a matching cookie. See **[`multi_machine_example.md`](multi_machine_example.md)** for a complete 3-machine walkthrough, the per-worker launch command, and prerequisites (hostname resolution, epmd/firewall, etc.).

## The two values that must match everywhere

`MR_COOKIE` and `MR_MASTER_NODE` must be identical on every node. If either differs, `Node.connect` fails silently and workers never register — the most common cause of a cluster that "starts but does nothing." Everything else (`MR_COORDS`, `MR_TEMP_DIR`, `MR_MIN_WORKERS`, `MR_WORKER_OUTPUT_DIR`) is per-process and may differ.
