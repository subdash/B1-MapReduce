defmodule MrProtocol.WorkerInfo do
  defstruct [:node, :coords, status: :idle]

  @type t :: %__MODULE__{
          node: atom(),
          coords: {float(), float()},
          status: :idle | :busy | :dead
        }
end
