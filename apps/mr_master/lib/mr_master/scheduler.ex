defmodule MrMaster.Scheduler do
  def assign_map_task(workers) do
    case Enum.find(workers, fn {_node, worker} ->
           worker.status == :idle
         end) do
      {node, _} -> node
      nil -> nil
    end
  end

  def assign_reduce_task(workers, map_worker_nodes) do
    # Assumes all nodes in map_worker_nodes are still in the workers registry.
    # A node could theoretically die after reporting map completion but before
    # reduce assignment; that race condition is handled by fault tolerance
    # (re-running the map task), not by this function.
    workers
    # Filter out non-idle workers
    |> Enum.filter(fn {_node, worker} ->
      worker.status == :idle
    end)
    # For each idle worker, build a list of distances to every map worker.
    # Get the mean distance, and then return a tuple of the node and its mean distance.
    |> Enum.map(fn {node, worker_info} ->
      distances =
        Enum.map(map_worker_nodes, fn map_node ->
          map_worker_coords = workers[map_node].coords
          MrProtocol.Distance.euclidean_distance(worker_info.coords, map_worker_coords)
        end)

      mean_distance = Enum.sum(distances) / length(distances)
      {node, mean_distance}
    end)
    |> case do
      # Check if list is empty after map (like when there is no idle worker) and return
      # nil if so.
      [] ->
        nil

      # Otherwise, pick the worker with the smallest mean distance
      candidates ->
        candidates
        |> Enum.min_by(fn {_node, mean_distance} -> mean_distance end)
        |> elem(0)
    end
  end
end
