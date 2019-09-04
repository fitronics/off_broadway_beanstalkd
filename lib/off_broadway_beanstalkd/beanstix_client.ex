defmodule OffBroadwayBeanstalkd.BeanstixClient do
  @moduledoc """
  Default SQS client used by `OffBroadwayBeanstalkd.Producer` to communicate with AWS
  SQS service. This client uses the `Beanstix` library and implements the
  `OffBroadwayBeanstalkd.BeanstalkdClient` and `Broadway.Acknowledger` behaviours which define
  callbacks for receiving and acknowledging messages.
  """

  alias Broadway.{Acknowledger, Message}
  require Logger

  @behaviour OffBroadwayBeanstalkd.BeanstalkdClient
  @behaviour Acknowledger

  @impl true
  def init(opts) do
    host = Keyword.get(opts, :host, '127.0.0.1')
    port = Keyword.get(opts, :port, 11300)
    tube = Keyword.get(opts, :tube, "default")
    {:ok, conn} = Beanstix.connect(host, port)

    if tube != "default" do
      {:ok, ^tube} = Beanstix.use(conn, tube)
      {:ok, 2} = Beanstix.watch(conn, tube)
      {:ok, 1} = Beanstix.ignore(conn, "default")
    end

    ack_ref = Broadway.TermStorage.put(%{conn: conn, tube: tube})

    {:ok, %{conn: conn, tube: tube, ack_ref: ack_ref}}
  end

  @impl true
  def receive_messages(demand, opts) do
    get_jobs(opts.conn, demand)
    |> wrap_jobs_as_messages(opts.ack_ref)
  end

  defp get_jobs(conn, demand) do
    cmds = List.duplicate({:reserve_with_timeout, 0}, demand)

    Beanstix.pipeline(conn, cmds)
    |> Enum.reduce([], fn job, acc ->
      case job do
        {:ok, :timed_out} -> acc
        {:ok, data} -> [data | acc]
        _ -> acc
      end
    end)
    |> Enum.reverse()
  end

  @impl true
  def ack(ack_ref, successful, failed) do
    opts = Broadway.TermStorage.get!(ack_ref)
    delete_successful_messages(opts.conn, successful)
    release_failed_messages(opts.conn, failed)
  end

  defp delete_successful_messages(conn, messages) do
    cmds =
      for %Message{acknowledger: {_, _, %{id: id}}} <- messages do
        {:delete, id}
      end

    if length(cmds) > 0 do
      Beanstix.pipeline(conn, cmds)
    end
  end

  # Release failed back to the queue with a delay of 30 seconds
  defp release_failed_messages(conn, messages) do
    cmds =
      for %Message{acknowledger: {_, _, %{id: id}}} <- messages do
        {:release, id, [delay: 30]}
      end

    if length(cmds) > 0 do
      Beanstix.pipeline(conn, cmds)
    end
  end

  defp wrap_jobs_as_messages(jobs, ack_ref) do
    Enum.map(jobs, fn {job_id, data} ->
      metadata = %{job_id: job_id}
      acknowledger = build_acknowledger(job_id, ack_ref)
      %Message{data: data, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp build_acknowledger(job_id, ack_ref) do
    {__MODULE__, ack_ref, %{id: job_id}}
  end
end
