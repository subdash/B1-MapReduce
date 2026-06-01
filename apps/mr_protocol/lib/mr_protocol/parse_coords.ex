defmodule MrProtocol.ParseCoords do
  def parse_coords(str) do
    str
    |> String.split(",")
    |> Enum.map(fn s ->
      {f, _rest} = Float.parse(s)
      f
    end)
  end
end
