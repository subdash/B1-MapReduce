ExUnit.start()

defmodule JobTest do
  use ExUnit.Case, async: true

  @tag :tmp_dir
  test "A new MapTask struct can be updated using the struct update syntax", %{tmp_dir: tmp_dir} do
    create_file = fn index ->
      path = Path.join(tmp_dir, "file#{index}")
      File.write!(path, "")
    end

    test_file_name = fn {map_task, index} ->
      assert map_task.file_path == Path.join(tmp_dir, "file#{index + 1}")
    end

    n = 5

    for index <- 1..n, do: create_file.(index)

    map_tasks = MrMaster.Job.create_map_tasks(tmp_dir, 5)

    # Test that all tasks start with idle status
    assert Enum.all?(map_tasks, fn task -> task.status == :idle end)
    # Test that filenames match expected format
    Enum.each(Enum.with_index(map_tasks), test_file_name)
    # Test that ids are sequential
    assert Enum.map(map_tasks, & &1.id) == Enum.to_list(1..length(map_tasks))
  end
end
