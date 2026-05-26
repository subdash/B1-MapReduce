defmodule MrWorker.RPC do
  def call(target_node, server, message, registry, throttle_multiplier \\ 1.0) do
    # If we have the nodes in the registry, we can calculate their (fictional) distance
    # and apply a delay in processing based on how far away they are from each other.
    # This allows us to test nearest-neighbor assignment in the master.
    if Map.has_key?(registry, node()) and Map.has_key?(registry, target_node) do
      calling_node_coords = registry[node()].coords
      target_node_coords = registry[target_node].coords

      euclidean_distance =
        MrProtocol.Distance.euclidean_distance(calling_node_coords, target_node_coords)

      delay = round(euclidean_distance * 2.0 * throttle_multiplier)
      Process.sleep(delay)
    end

    GenServer.call({server, target_node}, message)
  end
end
