ExUnit.start()

defmodule FileServerTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "fetches file contents as binary", %{tmp_dir: tmp_dir} do
    start_supervised!(MrWorker.FileServer)
    filepath = "#{tmp_dir}/file"
    :ok = File.write!(filepath, "abcd")
    binary = GenServer.call(MrWorker.FileServer, {:fetch, filepath})
    assert binary == "abcd"
  end

  test "returns error when file does not exist" do
    start_supervised!(MrWorker.FileServer)
    filepath = "non_existent/file"
    assert GenServer.call(MrWorker.FileServer, {:fetch, filepath}) == {:error, :not_found}
  end
end
