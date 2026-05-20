defmodule MrProtocol.MapTask do
  defstruct [:id, :file_path, :num_reducers, :assigned_to, status: :idle]

  @type t :: %__MODULE__{
          id: integer(),
          file_path: String.t(),
          num_reducers: integer(),
          assigned_to: atom() | nil,
          status: :idle | :in_progress | :completed
        }
end
