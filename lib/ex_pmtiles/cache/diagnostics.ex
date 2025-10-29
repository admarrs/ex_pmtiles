defmodule ExPmtiles.Cache.Diagnostics do
  @moduledoc """
  Diagnostic utilities for debugging cache issues.
  """

  @doc """
  Inspects the pending requests table for a given cache.

  Returns a list of pending tile requests with their waiting processes.
  """
  def inspect_pending(server) when is_pid(server) or is_atom(server) do
    name = resolve_server_name(server)
    table_name = :"#{name}_table"
    pending_table = :"#{table_name}_pending"

    case :ets.info(pending_table) do
      :undefined ->
        {:error, :table_not_found}

      _ ->
        build_pending_report(pending_table)
    end
  end

  defp build_pending_report(pending_table) do
    pending_entries = :ets.tab2list(pending_table)

    results =
      Enum.map(pending_entries, fn {tile_id, waiting_pids} ->
        build_entry_info(tile_id, waiting_pids)
      end)

    %{
      total_pending: length(results),
      entries: results
    }
  end

  defp build_entry_info(tile_id, waiting_pids) do
    alive_pids = Enum.filter(waiting_pids, &Process.alive?/1)
    dead_pids = Enum.filter(waiting_pids, fn pid -> not Process.alive?(pid) end)

    %{
      tile_id: tile_id,
      total_waiting: length(waiting_pids),
      alive_waiting: length(alive_pids),
      dead_waiting: length(dead_pids),
      waiting_pids: waiting_pids
    }
  end

  defp resolve_server_name(server) do
    cond do
      is_atom(server) -> server
      is_pid(server) -> GenServer.call(server, :get_name, 5000)
    end
  end

  @doc """
  Cleans up dead processes from the pending table.

  This can help recover from stuck pending entries.
  """
  def cleanup_dead_waiters(server) when is_pid(server) or is_atom(server) do
    name = resolve_server_name(server)
    table_name = :"#{name}_table"
    pending_table = :"#{table_name}_pending"

    case :ets.info(pending_table) do
      :undefined ->
        {:error, :table_not_found}

      _ ->
        cleanup_pending_entries(pending_table)
    end
  end

  defp cleanup_pending_entries(pending_table) do
    pending_entries = :ets.tab2list(pending_table)

    results =
      Enum.reduce(pending_entries, %{cleaned: 0, kept: 0}, fn {tile_id, waiting_pids}, acc ->
        process_pending_entry(pending_table, tile_id, waiting_pids, acc)
      end)

    {:ok, results}
  end

  defp process_pending_entry(pending_table, tile_id, waiting_pids, acc) do
    alive_pids = Enum.filter(waiting_pids, &Process.alive?/1)

    if Enum.empty?(alive_pids) do
      # No alive processes - delete the entry
      :ets.delete(pending_table, tile_id)
      %{acc | cleaned: acc.cleaned + 1}
    else
      # Update with only alive processes
      :ets.insert(pending_table, {tile_id, alive_pids})
      %{acc | kept: acc.kept + 1}
    end
  end

  @doc """
  Gets comprehensive cache diagnostics.
  """
  def get_diagnostics(server) when is_pid(server) or is_atom(server) do
    stats = ExPmtiles.Cache.get_stats(server)
    pending = inspect_pending(server)

    %{
      stats: stats,
      pending: pending,
      timestamp: DateTime.utc_now()
    }
  end
end
