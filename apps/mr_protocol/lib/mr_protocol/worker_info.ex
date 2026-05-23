defmodule MrProtocol.WorkerInfo do
  defstruct [:node, :coords, status: :idle, throttle_multiplier: 1.0]

  @type t :: %__MODULE__{
          node: atom(),
          coords: {float(), float()},
          status: :idle | :busy | :dead,
          throttle_multiplier: float()
        }
end
