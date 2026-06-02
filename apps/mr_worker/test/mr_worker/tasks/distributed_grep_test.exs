defmodule MrWorker.Tasks.DistributedGrepTest do
  use ExUnit.Case

  alias MrWorker.Tasks.DistributedGrep

  describe "map/2" do
    test "returns matching line as key-value pair when line contains pattern" do
      filename = "document.txt"
      line = "the quick brown fox"
      result = DistributedGrep.map(filename, line)
      assert result == [{filename, line}]
    end

    test "returns empty list when line does not contain pattern" do
      filename = "document.txt"
      line = "xyz abc def"
      result = DistributedGrep.map(filename, line)
      assert result == []
    end

    test "is case-insensitive" do
      filename = "document.txt"
      line = "The Quick Brown Fox"
      result = DistributedGrep.map(filename, line)
      assert result == [{filename, line}]
    end

    test "finds pattern at start of line" do
      filename = "doc.txt"
      line = "the fox jumps over lazy dog"

      result = DistributedGrep.map(filename, line)

      assert result == [{filename, line}]
    end

    test "finds pattern at end of line" do
      filename = "doc.txt"
      line = "the lazy brown fox"

      result = DistributedGrep.map(filename, line)

      assert result == [{filename, line}]
    end
  end

  describe "reduce/2" do
    test "combines multiple matching lines into a single output" do
      filename = "document.txt"
      lines = ["the fox jumps", "a fox appears", "another fox"]

      {returned_filename, result} = DistributedGrep.reduce(filename, lines)

      assert returned_filename == filename
      assert result == "the fox jumps | a fox appears | another fox"
    end

    test "handles single line" do
      filename = "document.txt"
      lines = ["the fox jumps"]

      {returned_filename, result} = DistributedGrep.reduce(filename, lines)

      assert returned_filename == filename
      assert result == "the fox jumps"
    end

    test "handles empty line list" do
      filename = "document.txt"
      lines = []

      {returned_filename, result} = DistributedGrep.reduce(filename, lines)

      assert returned_filename == filename
      assert result == ""
    end
  end

  test "implements MrProtocol.Task behaviour" do
    # function_exported?/3 doesn't load the module; ensure it's loaded first so the
    # assertion doesn't depend on test order (otherwise it's seed-dependent flaky).
    Code.ensure_loaded!(MrWorker.Tasks.DistributedGrep)
    assert :erlang.function_exported(MrWorker.Tasks.DistributedGrep, :map, 2)
    assert :erlang.function_exported(MrWorker.Tasks.DistributedGrep, :reduce, 2)
  end

  test "works with real file data from sample-data" do
    # Create a temp file with known content
    tmp_file = Path.join(System.tmp_dir!(), "grep_test_#{System.monotonic_time()}.txt")

    # Write 8 lines with "the"
    content = """
    the quick brown fox jumps
    over the lazy dog
    a fox appears in the forest
    the cat sleeps peacefully
    another fox moment
    birds flying high
    fox in the henhouse
    rabbits hop around
    the sly fox strikes
    the end of the story
    """

    File.write!(tmp_file, content)

    # Read the file and process it
    {:ok, file} = File.read(tmp_file)
    lines = String.split(file, "\n", trim: true)

    # Count how many lines match the pattern
    matches = Enum.flat_map(lines, fn line -> DistributedGrep.map(tmp_file, line) end)

    # Should find 5 matching lines
    assert length(matches) == 8

    # All matches should contain "fox"
    assert Enum.all?(matches, fn {_file, line} ->
             String.contains?(String.downcase(line), "the")
           end)

    # Verify specific expected lines are in the matches
    match_strings = Enum.map(matches, fn {_file, line} -> line end)
    assert Enum.member?(match_strings, "the quick brown fox jumps")
    assert Enum.member?(match_strings, "a fox appears in the forest")

    # Cleanup
    File.rm!(tmp_file)
  end
end
