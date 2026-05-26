ExUnit.start()

defmodule MrProtocolDistanceTest do
  use ExUnit.Case

  test "distance correctly calculates Euclidean distance between two points" do
    {coords1, coords2} = {{1.0, 1.0}, {4.0, 4.0}}
    dist = MrProtocol.Distance.euclidean_distance(coords1, coords2)
    # 18 because :math.pow((4 - 1), 2) == :math.pow(9)
    # So euclidean distance becomes :math.sqrt(9 + 9) == :math.sqrt(18)
    assert dist == :math.sqrt(18)
  end
end
