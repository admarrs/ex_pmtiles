defmodule ExPmtiles.CacheAvailabilityTest do
  use ExUnit.Case, async: false

  import ExPmtile.Support.BypassHelpers
  import Mox

  alias ExPmtiles.Cache
  alias ExPmtiles.CacheMock

  @bucket "test-bucket"
  @path "test.pmtiles"

  setup :verify_on_exit!
  setup [:start_bypass]

  setup %{bypass: bypass} do
    Mox.set_mox_global()

    stub(CacheMock, :new, fn region, bucket, path, _storage ->
      %ExPmtiles{
        region: region,
        bucket: bucket,
        path: path,
        source: :s3,
        directories: %{},
        pending_directories: %{}
      }
    end)

    stub(CacheMock, :zxy_to_tile_id, fn _z, _x, _y -> 1 end)

    stub(CacheMock, :get_zxy, fn pmtiles, z, x, y ->
      {{0, 100, "tile_data_#{z}_#{x}_#{y}"}, pmtiles}
    end)

    %{config: exaws_config_for_bypass(bypass)}
  end

  test "application supervision owns the shared cache task supervisor" do
    task_supervisor = Process.whereis(ExPmtiles.CacheTaskSupervisor)

    assert is_pid(task_supervisor)

    assert Enum.any?(Supervisor.which_children(ExPmtiles.Supervisor), fn
             {ExPmtiles.CacheTaskSupervisor, ^task_supervisor, :supervisor, [Task.Supervisor]} ->
               true

             _child ->
               false
           end)
  end

  test "keeps the cache alive while the shared task supervisor is unavailable" do
    {:ok, cache_pid} =
      Cache.start_link(
        name: :cache_without_task_supervisor,
        region: "us-east-1",
        bucket: @bucket,
        path: @path
      )

    Process.unlink(cache_pid)
    assert :ok = Supervisor.terminate_child(ExPmtiles.Supervisor, ExPmtiles.CacheTaskSupervisor)

    try do
      send(cache_pid, :check_file_changed)
      assert %{metadata_task: nil} = :sys.get_state(cache_pid)
    after
      if is_nil(Process.whereis(ExPmtiles.CacheTaskSupervisor)) do
        assert {:ok, _pid} =
                 Supervisor.restart_child(ExPmtiles.Supervisor, ExPmtiles.CacheTaskSupervisor)
      end

      if Process.alive?(cache_pid) do
        assert :ok = GenServer.stop(cache_pid)
      end
    end
  end

  test "serves tiles while a metadata check is waiting on S3", %{bypass: bypass, config: config} do
    caller = self()

    Bypass.expect(bypass, "HEAD", "/#{@bucket}/#{@path}", fn conn ->
      send(caller, :metadata_request_started)
      Process.sleep(1_000)

      response =
        conn
        |> Plug.Conn.put_resp_header("etag", "\"test-etag-123\"")
        |> Plug.Conn.resp(200, "")

      send(caller, :metadata_request_finished)
      response
    end)

    {:ok, pid} =
      Cache.start_link(
        name: :cache_metadata_blocking,
        region: "us-east-1",
        bucket: @bucket,
        path: @path,
        exaws_config: config
      )

    Process.unlink(pid)

    send(pid, :check_file_changed)
    assert_receive :metadata_request_started, 1_000

    task = Task.async(fn -> Cache.get_tile(:cache_metadata_blocking, 0, 0, 0) end)
    result = Task.yield(task, 500) || Task.shutdown(task, :brutal_kill)

    assert result == {:ok, {:ok, "tile_data_0_0_0"}}
    assert_receive :metadata_request_finished, 2_000
    assert :ok = GenServer.stop(pid)
  end
end
