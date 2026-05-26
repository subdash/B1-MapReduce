defmodule MrProtocol.Distance do
  def euclidean_distance(coords1, coords2) do
    {x1, y1} = coords1
    {x2, y2} = coords2
    xdelta = x2 - x1
    ydelta = y2 - y1
    # Return Euclidean distance: the square root of the sum
    # of the squares of the x and y deltas 
    :math.sqrt(:math.pow(xdelta, 2) + :math.pow(ydelta, 2))
  end
end
