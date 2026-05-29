# Fault Tolerance: Stale Reduce Task Locations

**Date:** 2026-05-29  
**Status:** Design (not yet implemented)  
**Priority:** High — affects correctness during worker failures in reduce phase

---

## Problem

When a map worker dies **during the reduce phase**, the master correctly re-queues affected map tasks back to `:idle` and the reduce tasks back to `:idle`. However, the reduce tasks' `locations` lists still contain references to the dead worker's intermediate files.

If a reduce task is re-assigned and re-executed, it tries to fetch from the dead node and fails:

```elixir
{:dead_node@localhost, "tmp/dead_node@localhost/map-3-bucket-1.bin"}
```

The RPC call hangs or returns an error. The reduce task crashes, gets re-queued again, and loops forever. The corresponding map task sits in `:idle` state, never running again because `assign_pending_tasks` during `:reducing` phase only processes reduce tasks, not map tasks.

**Result:** Job hangs indefinitely. Correctness is lost — some data is missing from the output.

---

## Solution: Roll Back to Mapping Phase

Follow the paper's fault tolerance model (section 3.3): when a map worker dies during reduce, **abort the reduce phase** and return to `:mapping` to re-run affected map tasks.

### Algorithm

1. In `handle_info({:nodedown, node}, state)`, after re-queuing tasks:
   - Check if `state.phase == :reducing`
   - Find all reduce tasks whose `locations` list contains tuples with `node` as the first element
   - Mark those reduce tasks as `:idle` and clear their `locations` lists
   - Set `state.phase = :mapping`
   - Log: `[master] phase_rolled_back_to_mapping | node=worker3@localhost reason=map_worker_died_during_reduce`

2. Call `assign_pending_tasks(state)` — it will now see idle map tasks and assign them to surviving workers.

3. Once all map tasks are `:completed` again, `maybe_start_reduce` rebuilds the reduce task locations from scratch (now excluding the dead worker's data) and transitions back to `:reducing`.

### Why This Works

- **Completeness:** Re-running map tasks ensures all output is reprocessed. Reduce tasks see the full dataset (minus the dead worker's input, which is acceptable — we lose that worker's documents but not intermediate inconsistency).
- **Simplicity:** Single phase roll-back is easier than trying to patch reduce task locations mid-execution.
- **Correctness:** Matches the paper's fault tolerance design.

### Trade-off

**Cost:** If a map worker dies late in the reduce phase (e.g., 95% complete), we throw away reduce progress and start over. Mitigated by:
- Backup task execution (if enabled) — backups complete before primary failure causes issues
- Rare occurrence — worker death during reduce is uncommon if workers are healthy
- Correctness > performance — wrong answers are worse than slow answers

---

## Implementation Steps

### 1. Add Phase Transition Logic to `handle_info({:nodedown, node}, state)`

After re-queuing in-progress/completed map and reduce tasks:

```elixir
if state.phase == :reducing do
  # Find reduce tasks affected by this node's death
  affected_reduce_task_ids =
    Enum.filter(state.reduce_tasks, fn {_id, task} ->
      Enum.any?(task.locations, fn {map_worker_node, _file_path} -> 
        map_worker_node == node 
      end)
    end)
    |> Enum.map(fn {id, _task} -> id end)
  
  # Re-queue affected reduce tasks
  updated_reduce_tasks =
    Map.new(state.reduce_tasks, fn {task_id, task} ->
      if task_id in affected_reduce_task_ids do
        {task_id, %MrProtocol.ReduceTask{task | status: :idle, assigned_to: nil, locations: []}}
      else
        {task_id, task}
      end
    end)
  
  state = put_in(state.reduce_tasks, updated_reduce_tasks)
  state = %{state | phase: :mapping}
  state = log_event(state, "[master] phase_rolled_back_to_mapping | node=#{node} reason=map_worker_died_during_reduce")
else
  state
end
```

### 2. Update `maybe_start_reduce`

Ensure it clears the `intermediate_locations` map so that reduce tasks are built fresh the next time all map tasks complete:

```elixir
if Enum.all?(Map.values(state.map_tasks), map_task_is_complete) do
  # If we're rolling back from reduce phase, clear stale intermediate locations
  state = if state.phase == :mapping, do: %{state | intermediate_locations: %{}}, else: state
  
  # ... rest of reduce task creation ...
```

Actually, a simpler approach: in the roll-back logic above, also clear `intermediate_locations`:

```elixir
state = %{state | phase: :mapping, intermediate_locations: %{}}
```

This ensures no stale data contaminates the rebuild.

### 3. Tests

Add a test simulating:
1. Submit a job
2. All map tasks complete
3. Some reduce tasks start
4. A map worker dies while reduce is in progress
5. Verify: phase rolls back to `:mapping`, affected reduce tasks go `:idle` with empty locations, affected map tasks are re-queued
6. Verify: job eventually completes with correct output

---

## Edge Cases

### Q: What if the map worker dies *after* its intermediate files have been fully fetched by all reduce tasks?

A: Doesn't matter. The files are in memory on the reduce workers. They'll complete and write output. Only reduce tasks with locations pointing to the dead node get rolled back.

### Q: What if multiple map workers die simultaneously?

A: Treat each independently. Reduce tasks accumulate dead node references; when the roll-back fires, all affected reduce tasks go `:idle` at once. Map tasks are re-queued. Normal operation resumes.

### Q: What if a map worker dies during mapping, then a second map worker dies during reduce?

A: First death re-queues map tasks, job stays in `:mapping`. Second death does the same. No phase roll-back needed on the second one because phase is still `:mapping`.

---

## Verification

Run the end-to-end test with fault injection:

```bash
# Terminal 1: Start the job
mix mr.start --workers 6 --task WordCount --input sample-data/

# Terminal 2: Wait for reduce phase to start (watch logs), then kill a map worker
kill -9 <worker_pid>

# Verify logs show:
# [master] phase_rolled_back_to_mapping | ...
# [master] task_assigned | type=map id=X ...  (re-assignments)
# Eventually: [master] job_complete | ...
```

Spot-check output files to ensure word counts are correct (no duplicates, no missing words).
