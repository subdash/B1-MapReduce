ExUnit.start()

defmodule ReduceTaskTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "ReduceTask execute method does the thing", %{tmp_dir: tmp_dir} do
    map_output1 =
      :erlang.term_to_binary([
        {"pikachu", 3},
        {"charizard", 2},
        {"dragonite", 1}
      ])

    map_output2 =
      :erlang.term_to_binary([
        {"pikachu", 1},
        {"charizard", 1},
        {"raichu", 3},
        {"charmeleon", 5}
      ])

    file_path1 = Path.join(tmp_dir, "file1.bin")
    file_path2 = Path.join(tmp_dir, "file2.bin")
    File.write!(file_path1, map_output1)
    File.write!(file_path2, map_output2)

    task = %MrProtocol.ReduceTask{
      locations: [
        {node(), file_path1},
        {node(), file_path2}
      ],
      assigned_to: node(),
      bucket: 1
    }

    registry = %{node() => %MrProtocol.WorkerInfo{node: node(), coords: {0.0, 0.0}}}

    execute_result =
      MrWorker.ReduceTask.execute(task, MrWorker.Tasks.WordCount, registry, tmp_dir, 1.0)

    assert execute_result == :ok

    file_contents = File.read!("#{tmp_dir}/bucket-#{task.bucket}.txt")
    assert file_contents == "charizard\t3\ncharmeleon\t5\ndragonite\t1\npikachu\t4\nraichu\t3\n"
  end
end
