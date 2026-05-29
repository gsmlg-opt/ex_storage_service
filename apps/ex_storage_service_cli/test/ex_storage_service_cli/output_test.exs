defmodule ExStorageServiceCli.OutputTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.Output

  describe "format_bytes_short/1" do
    test "formats various sizes without spaces and with short units" do
      assert Output.format_bytes_short(0) == "0B"
      assert Output.format_bytes_short(36) == "36B"
      assert Output.format_bytes_short(1024) == "1.0KB"
      assert Output.format_bytes_short(1024 * 1024) == "1.0MB"
      assert Output.format_bytes_short(1024 * 1024 * 1024) == "1.0GB"
      assert Output.format_bytes_short(nil) == "0B"
    end
  end

  describe "format_datetime_local/1" do
    test "formats ISO 8601 UTC string to local format" do
      # Note: Local time conversion output is timezone-dependent, so we check formatting pattern
      res = Output.format_datetime_local("2026-01-02T17:59:04Z")
      assert String.starts_with?(res, "[2")
      assert String.ends_with?(res, "]")
      assert String.length(res) >= 23
      # It should contain standard date-time separators
      assert res =~ ~r/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \w+\]/
    end

    test "handles malformed values gracefully" do
      assert Output.format_datetime_local("malformed") == "malformed"
      assert Output.format_datetime_local("") == ""
      assert Output.format_datetime_local(nil) == ""
    end
  end

  describe "current_datetime_local/0" do
    test "returns current time in local format" do
      res = Output.current_datetime_local()
      assert res =~ ~r/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \w+\]/
    end
  end
end
