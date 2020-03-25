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

    requeue = Keyword.get(opts, :requeue, :always)
    requeue_delay_min = Keyword.get(opts, :requeue_delay_min, 10)
    requeue_delay_max = Keyword.get(opts, :requeue_delay_max, 60)

    ack_ref =
      Broadway.TermStorage.put(%{
        conn: conn,
        tube: tube,
        requeue: requeue,
        requeue_delay_min: requeue_delay_min,
        requeue_delay_max: requeue_delay_max
      })

    {:ok, %{conn: conn, tube: tube, ack_ref: ack_ref, requeue: requeue}}
  end

  @impl true
  def receive_messages(demand, opts) do
    get_jobs(opts.conn, demand)
    |> wrap_jobs_as_messages(opts)
  end

  defp get_jobs(conn, demand) do
    cmds = List.duplicate({:reserve_with_timeout, 0}, demand)

    Beanstix.pipeline(conn, cmds, 10000)
    |> Enum.reduce([], fn job, acc ->
      case job do
        {:ok, :timed_out} -> acc
        {:ok, :deadline_soon} -> acc
        {:ok, data} -> [data | acc]
        _ -> acc
      end
    end)
    |> Enum.reverse()
  end

  @impl true
  def ack(ack_ref, successful, failed) do
    opts = Broadway.TermStorage.get!(ack_ref)
    delete_messages(opts.conn, successful)
    process_failed_messages(opts.conn, failed, opts)
  end

  defp delete_messages(_, []), do: true

  defp delete_messages(conn, messages) do
    cmds =
      for %Message{metadata: metadata} <- messages do
        {:delete, metadata.job_id}
      end

    Beanstix.pipeline(conn, cmds)
  end

  # Release/Delete failed messages depending on requeue and how many times it has already been releases
  defp process_failed_messages(_, [], _), do: true

  defp process_failed_messages(conn, messages, opts) do
    cmds =
      for %Message{metadata: metadata} <- messages do
        case metadata.requeue do
          :never ->
            {:delete, metadata.job_id}

          _ ->
            case Beanstix.stats_job(conn, metadata.job_id) do
              {:ok, stats_job} when is_map(stats_job) ->
                calculate_cmd_for_failed_message(metadata, opts, stats_job["releases"])

              _ ->
                nil
            end
        end
      end
      |> Enum.reject(fn x -> x == nil end)

    Beanstix.pipeline(conn, cmds)
  end

  defp calculate_cmd_for_failed_message(metadata, opts, releases) do
    delay = calculate_delay(opts, releases)

    case {metadata.requeue, releases} do
      {:once, 0} -> {:release, metadata.job_id, [delay: delay]}
      {x, releases} when is_integer(x) and releases < x -> {:release, metadata.job_id, [delay: delay]}
      _ -> {:delete, metadata.job_id}
    end
  end

  defp calculate_delay(opts, releases) do
    delay = round(:math.pow(2, releases) * opts.requeue_delay_min)

    if delay > opts.requeue_delay_max do
      opts.requeue_delay_max
    else
      delay
    end
  end

  defp wrap_jobs_as_messages(jobs, opts) do
    Enum.map(jobs, fn {job_id, data} ->
      metadata = %{job_id: job_id, requeue: opts.requeue}
      acknowledger = build_acknowledger(job_id, opts.ack_ref)
      %Message{data: data, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp build_acknowledger(job_id, ack_ref) do
    {__MODULE__, ack_ref, %{job_id: job_id}}
  end
end
