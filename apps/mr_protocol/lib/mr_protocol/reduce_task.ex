defmodule MrProtocol.ReduceTask do
  defstruct [:id, :bucket, :locations, :assigned_to, status: :idle]

  @type t :: %__MODULE__{
          id: integer(),
          bucket: integer(),
          locations: [{atom(), String.t()}],
          assigned_to: atom() | nil,
          status: :idle | :in_progress | :completed
        }
end
