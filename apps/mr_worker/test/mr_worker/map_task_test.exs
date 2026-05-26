ExUnit.start()

defmodule MapTaskTest do
  use ExUnit.Case, async: true

  test "word count map task creates files as expected" do
    test_tmp = System.tmp_dir!()
    worker_name = "test_worker"
    test_input = Path.join(test_tmp, "test_input.txt")
    File.write!(test_input, "hello world\nhello again\n")

    map_task = %MrProtocol.MapTask{
      id: 1,
      file_path: test_input,
      num_reducers: 1
    }

    {:ok, bucket_locations} =
      MrWorker.MapTask.execute(map_task, MrWorker.Tasks.WordCount, worker_name)

    Enum.each(bucket_locations, fn {_bucket, filepath} ->
      # Verify each file exists
      assert File.exists?(filepath)
      {:ok, binary} = File.read(filepath)
      # Verify file is not empty
      assert byte_size(binary) > 0
      pairs = :erlang.binary_to_term(binary)
      # Assert file contains list
      assert is_list(pairs)
      # Assert pairs map binary to integer
      assert Enum.all?(pairs, fn {k, v} -> is_binary(k) and is_integer(v) end)
    end)

    # Cleanup tmp files
    File.rm_rf!(Path.join(test_tmp, worker_name))
  end

  test "map task combines duplicate keys if combine/2 is defined" do
    test_tmp = System.tmp_dir!()
    worker_name = "test_worker"
    test_input = Path.join(test_tmp, "input.txt")
    File.write!(test_input, "chicken chicken chicken\n")

    map_task = %MrProtocol.MapTask{id: 1, file_path: test_input, num_reducers: 1}

    {:ok, bucket_locations} =
      MrWorker.MapTask.execute(map_task, MrWorker.Tasks.WordCount, worker_name)

    # Decode and verify only one entry for "chicken" with combined count
    pairs = bucket_locations[0] |> File.read!() |> :erlang.binary_to_term()
    assert pairs == [{"chicken", 3}]

    # Cleanup tmp files
    File.rm_rf!(Path.join(test_tmp, worker_name))
  end

  test "map task still returns bucket locations map for empty file" do
    test_tmp = System.tmp_dir!()
    worker_name = "test_worker"
    test_input = Path.join(test_tmp, "input.txt")
    File.write!(test_input, "")

    map_task = %MrProtocol.MapTask{id: 1, file_path: test_input, num_reducers: 1}

    {:ok, bucket_locations} =
      MrWorker.MapTask.execute(map_task, MrWorker.Tasks.WordCount, worker_name)

    assert is_map(bucket_locations)

    # Cleanup tmp files
    File.rm_rf!(Path.join(test_tmp, worker_name))
  end
end
