# Baby's First MapReduce

This is an implementation of the MapReduce programming model as defined in the 2004 paper [_MapReduce: Simplified Data Processing on Large Clusters_](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

The framework runs MapReduce jobs on a cluster of Erlang nodes. Currently it executes on a single machine, but the distributed architecture is designed to extend to multiple physical machines. The initial task is a word-frequency count across 20 large text files. A Phoenix LiveView dashboard provides real-time visualization of task progress across workers.

<img width="3456" height="2070" alt="demo_final_clip" src="https://github.com/user-attachments/assets/b58e05fb-096f-49fb-a896-3025df1f9107" />

## How it works

A B1 MapReduce cluster runs a master server whose high-level responsibility is to receive job submissions and orchestrate their completion. Worker nodes register with the master server and are assigned tasks -- map tasks or reduce tasks depending on the phase of the job.

Map tasks involve processing a segment of a dataset and writing the output to an intermediate file on the map worker's local disk. An optional combine function may be provided that combines the output in memory before writing it to a file. The combine step is an optimization that reduces network traffic: instead of sending millions of pairs to reducers, we pre-aggregate locally.

Once all map tasks have been completed, the reduce phase begins, where reduce tasks point to the intermediate files from the mapping phase. Those intermediate files are fetched via RPC from the map nodes. The reduce tasks aggregate data from various map tasks.

A "word count" map task provides a simple example:

- Map phase: transform every word into a tuple, e.g. `{"the", 1}`
- Combine: combine all of the counts, e.g. `[{"the", 1}, {"the", 1}]` become `[{"the", 2}]`
- Reduce: same as combine, but across multiple files

When a reduce task completes successfully, it writes its output to a bucket and that's it. However, if a map worker node dies during the reduce phase and before its intermediate files have been fully fetched, both the reduce tasks and map tasks must be re-queued, since the intermediate files required for the reduce task have become inaccessible.

Locality becomes important during the reduce phase since RPC calls between reduce workers and the nodes containing their intermediate files become more latent the further away they are in the network topology. The master takes into account the distance between nodes (this is simulated in this framework by assigning coordinates to nodes) when assigning reduce tasks by prioritizing reduce workers whose mean distance to the source map workers is the smallest.

<img width="1256" height="1682" alt="B1 MapReduce phase diagram" src="https://github.com/user-attachments/assets/8017736a-8173-41d7-bbcc-1a2bc1d576d1" />

## Limitations, tradeoffs and improvements
- **One map task per file**: A more efficient implementation would read byte ranges of a configurable size from files. This would reduce the chance of failure when processing large files and allow for much higher parallelism at the cost of the minor additional overhead of more map tasks to manage and more RPC calls during the reduce phase.
- **Can only run on one machine**: With some minor modifications, we could extend this framework to assign tasks to nodes on remote machines so that it could theoretically be used for production workloads. The current implementation however assumes that all worker nodes live on the same machine as the master node, so parallelism is limited by the resources of the machine it runs on.
- **Stale file locations**: When a map worker dies during the reduce phase, the locations of the map workers intermediate files refer to a node that is now dead, so the files can no longer be fetched. The worker that picks up the reduce task will get stuck because it will be unable to fetch those intermediate files. The fix is to roll the job back to the mapping phase and re-assign the map task, rewriting those intermediate files and submitting valid new locations to the master.
- **Tasks are not configurable**: There is currently no mechanism to provide configuration for a task outside of the actual Elixir code that defines the task module. So for example, if I wanted the distributed grep job to search for a word other than "the", I would need to edit the Elixir code for that task. This creates an undesirable operational burden.
- **Node distance is simulated**: A real implementation would assign reduce tasks based on how latent the RPC calls are rather than simulating distance between nodes via bogus coordinates.

## Prerequisites

- **Elixir 1.14+** and **Erlang/OTP 24+**
- **Python 3.x** (for generating sample data)
- ~2 GB of disk space (for sample data and intermediate files)

## Quick Start

### 1. Generate Sample Data

Generate 20 sample text files (~1 GB total) using the included Python script:

```bash
python3 generate_data.py
```

This creates `sample-data/document_01.txt` through `document_20.txt`, each containing 700k–1.2M lines of randomly selected words. The script takes a few minutes to complete.

### 2. Start the Application

Install dependencies and start the Phoenix LiveView dashboard:

```bash
mix deps.get
mix ecto.create
mix phx.server
```

This starts:

- **Phoenix LiveView dashboard** — available at `http://localhost:4000` with real-time visualization of job progress

### 3. Run the Word Count Job

In a separate terminal, start the master node and worker cluster, then execute the job:

```bash
mix mr.start
```

This:

1. Starts the master node and 4 worker nodes (configurable with `--workers`)
2. Assigns map tasks to workers (one per file)
3. Emits intermediate `{word, 1}` pairs bucketed by word hash
4. Assigns reduce tasks to aggregate counts per word
5. Writes final results to `output/`

You can customize the run with options:

```bash
mix mr.start --workers 8 --reducers 4 --input sample-data/
```

Watch real-time progress on the dashboard at `http://localhost:4000`:

![LiveView Dashboard Demo](docs/live_view_demo_gif.gif)

The dashboard shows all active workers, their current task assignments, and real-time job progress.

## AI disclaimer

Claude Code was used in the development of this project for the following tasks. Almost all of the worker, master and protocol code was written by hand. The dashboard code was mainly written by an LLM. The documentation here (in the README) was written by hand with the assistance of an LLM, while the implementation plans in the docs folder were written entirely by an LLM. Additionally, once the master/worker/protocol code was complete, heavy LLM usage was utilized in order to orchestrate creating a cluster and running a job (cf. mr.start mix task).
