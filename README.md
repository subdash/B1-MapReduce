# Baby's First MapReduce

This is an implementation of the MapReduce programming model as defined in the 2004 paper [_MapReduce: Simplified Data Processing on Large Clusters_](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

The framework runs MapReduce jobs on a cluster of Erlang nodes. It runs on a single machine for development and scales across multiple physical machines over a LAN with no code changes — workers are distributed Erlang nodes that connect to the master by name. The initial task is a word-frequency count across 20 large text files. A Phoenix LiveView dashboard provides real-time visualization of task progress across workers.

<img width="3456" height="2070" alt="demo_final_clip" src="https://github.com/user-attachments/assets/b58e05fb-096f-49fb-a896-3025df1f9107" />

## How it works

A B1 MapReduce cluster runs a master server whose high-level responsibility is to receive job submissions and orchestrate their completion. Worker nodes register with the master server and are assigned tasks -- map tasks or reduce tasks depending on the phase of the job.

Map tasks involve processing a segment of a dataset and writing the output to an intermediate file on the map worker's local disk. An optional combine function may be provided that combines the output in memory before writing it to a file. The combine step is an optimization that reduces network traffic: instead of sending millions of pairs to reducers, we pre-aggregate locally.

Once all map tasks have been completed, the reduce phase begins, where reduce tasks point to the intermediate files from the mapping phase. Those intermediate files are fetched via RPC from the map nodes. The reduce tasks aggregate data from various map tasks.

A "word count" map task provides a simple example:

- Map phase: transform every word into a tuple, e.g. `{"the", 1}`
- Combine: combine all of the counts, e.g. `[{"the", 1}, {"the", 1}]` become `[{"the", 2}]`
- Reduce: same as combine, but across multiple files

When a reduce task completes successfully, the reduce worker writes its output (one file per reduce bucket) to its own local disk. However, if a map worker node dies during the reduce phase and before its intermediate files have been fully fetched, both the reduce tasks and map tasks must be re-queued, since the intermediate files required for the reduce task have become inaccessible.

Locality becomes important during the reduce phase since RPC calls between reduce workers and the nodes containing their intermediate files become more latent the further away they are in the network topology. The master takes into account the distance between nodes (this is simulated in this framework by assigning coordinates to nodes) when assigning reduce tasks by prioritizing reduce workers whose mean distance to the source map workers is the smallest.

<img width="1256" height="1682" alt="B1 MapReduce phase diagram" src="https://github.com/user-attachments/assets/8017736a-8173-41d7-bbcc-1a2bc1d576d1" />

## Limitations, tradeoffs and improvements
- **One map task per file**: A more efficient implementation would read byte ranges of a configurable size from files. This would reduce the chance of failure when processing large files and allow for much higher parallelism at the cost of the minor additional overhead of more map tasks to manage and more RPC calls during the reduce phase.
- **No distributed filesystem**: Real MapReduce reads input from and writes output to a shared distributed filesystem (GFS). This implementation has none, so input files must be present on every worker machine at the same path, and each reduce worker writes its final output to its own local disk. On a multi-machine run that leaves the output scattered across the worker machines (gather it with `scp`/`rsync`, or point `MR_WORKER_OUTPUT_DIR` at a shared mount); on a single-machine run it all lands in `output/`. That keeps a multi-machine run to zero shared infrastructure. See [`docs/MULTI_MACHINE_SETUP.md`](docs/MULTI_MACHINE_SETUP.md).
- **Tasks are not configurable**: There is currently no mechanism to provide configuration for a task outside of the actual Elixir code that defines the task module. So for example, if I wanted the distributed grep job to search for a word other than "the", I would need to edit the Elixir code for that task. This creates an undesirable operational burden.
- **Node distance is simulated**: A real implementation would assign reduce tasks based on how latent the RPC calls are rather than simulating distance between nodes via bogus coordinates.

## Prerequisites

- **Elixir 1.20-rc** on **Erlang/OTP 29** — the exact versions are pinned in `.tool-versions`; with [asdf](https://asdf-vm.com) installed, run `asdf install` to match them.
- **Python 3.x** (for generating sample data)
- ~2 GB of disk space (for sample data and intermediate files)

## Quick Start

### 1. Generate Sample Data

Generate 20 sample text files (~1 GB total) using the included Python script:

```bash
python3 scripts/generate_data.py
```

This creates `sample-data/document_01.txt` through `document_20.txt`, each containing 700k–1.2M lines of randomly selected words. The script takes a few minutes to complete. It is seeded, so every run — and every machine — produces an identical dataset; pass a different seed with `python3 scripts/generate_data.py <seed>` if you want different data.

### 2. Install Dependencies

```bash
mix deps.get
```

### 3. Run the Word Count Job

Start the master, spawn a local worker cluster, and run the job — all with one command:

```bash
mix mr.start
```

This:

1. Starts the master node and 4 local worker nodes (configurable with `--workers`)
2. Starts the Phoenix LiveView dashboard at `http://localhost:4000`
3. Assigns map tasks to workers (one per file)
4. Emits intermediate `{word, 1}` pairs bucketed by word hash
5. Assigns reduce tasks to aggregate counts per word
6. Each reduce worker writes its final results to local disk; on a single-machine run they all land in `output/`

You can customize the run with options:

```bash
mix mr.start --workers 8 --reducers 4 --input sample-data/
```

Watch real-time progress on the dashboard at `http://localhost:4000` — it shows all active workers, their current task assignments, and overall job progress.

### 4. Run Across Multiple Machines (optional)

The same code runs as a real cluster across several machines with no changes: start the master with `mix mr.start --distributed` on one machine and `mix mr.worker` on the others. See **[`docs/MULTI_MACHINE_SETUP.md`](docs/MULTI_MACHINE_SETUP.md)** for the full walkthrough (node names, cookies, networking, troubleshooting) and **[`config/README.md`](config/README.md)** for the run modes and configuration.

## AI disclaimer

Claude Code was used in the development of this project for the following tasks. Almost all of the worker, master and protocol code was written by hand. The dashboard code was mainly written by an LLM. The documentation here (in the README) was written by hand with the assistance of an LLM, while the implementation plans in the docs folder were written entirely by an LLM. Additionally, once the master/worker/protocol code was complete, heavy LLM usage was utilized in order to orchestrate creating a cluster and running a job (cf. mr.start mix task).
