# Baby's First MapReduce

## TL;DR
This is an Elixir implementation of MapReduce, which is a framework developed by Google for processing large data sets. Map and reduce functions are defined for a task, input data is provided, and worker nodes join the single-master cluster. The map function processes data and writes the output to a file. The reduce function aggregates the output of the map tasks. In Google MapReduce, files are read from and written to a shared filesystem (GFS). In my implementation, input data is duplicated on each machine and output data is written to the local disk of the workers.

## What this project is

This is an implementation of the MapReduce programming model as defined in the 2004 paper [_MapReduce: Simplified Data Processing on Large Clusters_](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

The framework runs MapReduce jobs on a cluster of Erlang nodes. It runs on a single machine for development and scales across multiple physical machines over a LAN — workers are distributed Erlang nodes that connect to the master by name. The two tasks currently implemented are a word-frequency count and distributed grep. A Phoenix LiveView dashboard provides real-time visualization of task progress across workers.

I created this project as a way to learn Elixir, Erlang OTP, and get my hands dirty with distributed systems. It was very satisfying to have the opportunity to apply a lot of the theory I learned from textbooks and research papers to a real system I designed and built.

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

<img width="1256" height="1682" alt="B1 MapReduce phase diagram" src="https://github.com/user-attachments/assets/5732ba6c-a4ee-4938-b879-8d10fc993c77" />

## Limitations, tradeoffs and improvements
- **One map task per file**: A more efficient implementation would read byte ranges of a configurable size from files. This would reduce the chance of failure when processing large files and allow for much higher parallelism at the cost of the minor additional overhead of more map tasks to manage and more RPC calls during the reduce phase.
- **Single master**: While faithful to the paper, the design choice of having a single master has obvious drawbacks, namely that if the master dies, all jobs that is orchestrating must be restarted and re-run from the beginning. Google decided that a master failure was rare enough to justify the single-master design.
- **No distributed filesystem**: Real MapReduce reads input from and writes output to a shared distributed filesystem (GFS). This implementation has none, so input files must be present on every worker machine at the same path, and each reduce worker writes its final output to its own local disk. On a multi-machine run that leaves the output scattered across the worker machines (gather it with `scp`/`rsync`, or point `MR_WORKER_OUTPUT_DIR` at a shared mount); on a single-machine run it all lands in `output/`. That keeps a multi-machine run to zero shared infrastructure. See [`docs/MULTI_MACHINE_SETUP.md`](docs/MULTI_MACHINE_SETUP.md).
- **Tasks are not configurable**: There is currently no mechanism to provide configuration for a task outside of the actual Elixir code that defines the task module. So for example, if I wanted the distributed grep job to search for a word other than "the", I would need to edit the Elixir code for that task. This creates an undesirable operational burden.
- **Node distance is simulated**: A real implementation would assign reduce tasks based on their relative distance in the network topology rather than simulating distance between nodes via bogus coordinates.
- **Operator burden**: All machines running either the master or worker nodes must have the project cloned and the Elixir/Erlang dependencies installed. All processes are started manually. For me to set up a cluster, I had to clone the project on three different computers, install the same versions of dependencies on each one, manually discover the local hostnames for each and write out the correct commands to connect one node to another within the network. This process could be greatly simplified with an install script and a more user friendly run script. Node discovery would be a nice improvement too so that workers don't have to manually connect to the master. The current process is error prone and rather cumbersome.

## Prerequisites

The following instructions describe how to run a MapReduce task on a single machine. For multi-machine setup instructions, see [here](docs/MULTI_MACHINE_SETUP.md).

- **Elixir 1.20-rc** on **Erlang/OTP 29** — the exact versions are pinned in `.tool-versions`; with [asdf](https://asdf-vm.com) installed, run `asdf install` to match them.
- **Python 3.x** (for generating sample data)
- ~2 GB of disk space (for sample data and intermediate files)

## Quick Start

### 1. Generate Sample Data

Generate 20 sample text files (~1 GB total) using the included Python script:

```bash
python3 scripts/generate_data.py
```

This creates `sample-data/document_01.txt` through `document_20.txt`, each containing 700k–1.2M lines of randomly selected words. The script may take a few minutes to complete. It is seeded, so every run — and every machine — produces an identical dataset; pass a different seed with `python3 scripts/generate_data.py <seed>` if you want different data.

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

Claude Code was used to code most of the LiveView dashboard and a small portion of the worker/protocol/master code, as well as to improve documentation. Since the whole point of this project was to learn Elixir and learn about distributed systems, Claude was utilized mainly for educational purposes rather than speed of development.
