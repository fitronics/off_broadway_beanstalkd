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

    {:ok, %{conn: conn, ack_ref: ack_ref}}
  end

  @impl true
  def receive_messages(demand, opts) do
    receive_messages(demand, opts, [])
    |> wrap_received_messages(opts.ack_ref)
  end

  def receive_messages(0, _, acc), do: {:ok, Enum.reverse(acc)}

  def receive_messages(demand, opts, acc) do
    case Beanstix.reserve(opts.conn, 0) do
      {:ok, :timed_out} -> {:ok, Enum.reverse(acc)}
      {:ok, job} -> receive_messages(demand - 1, opts, [job | acc])
      error -> error
    end
  end

  @impl true
  def ack(ack_ref, successful, _failed) do
    opts = Broadway.TermStorage.get!(ack_ref)
    delete_messages(opts.conn, successful)
  end

  defp delete_messages(conn, messages) do
    cmds =
      for %Message{acknowledger: {_, _, %{id: id}}} <- messages do
        {:delete, id}
      end

    Beanstix.pipeline(conn, cmds)
  end

  defp wrap_received_messages({:ok, jobs}, ack_ref) do
    Enum.map(jobs, fn {job_id, data} ->
      metadata = %{job_id: job_id}
      acknowledger = build_acknowledger(job_id, ack_ref)
      %Message{data: data, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp wrap_received_messages({:error, reason}, _) do
    Logger.error("Unable to fetch events from beanstald. Reason: #{inspect(reason)}")
    []
  end

  defp build_acknowledger(job_id, ack_ref) do
    {__MODULE__, ack_ref, %{id: job_id}}
  end
end
