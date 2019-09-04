defmodule OffBroadwayBeanstalkd.OffBroadwayBeanstalkd.IntegrationTest do
  use ExUnit.Case

  alias Broadway.Message

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data, message.metadata})

      case message.data =~ "Failed" do
        true -> Message.failed(message, "Failed")
        _ -> message
      end
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  describe "integration" do
    setup do
      tube = tube_name()
      {:ok, pid} = start_broadway(tube)
      {:ok, conn} = Beanstix.connect()
      {:ok, ^tube} = Beanstix.use(conn, tube)
      %{pid: pid, conn: conn, tube: tube}
    end

    test "receive messages when the queue has less than the demand", %{pid: pid, conn: conn, tube: tube} do
      jobs = for i <- 1..5, do: {:put, "Message #{i}"}
      Beanstix.pipeline(conn, jobs)

      for i <- 1..5 do
        msg = "Message #{i}"
        assert_receive {:message_handled, ^msg, _}
      end

      cleanup(pid, conn, tube)
    end

    test "keep receiving messages when the queue has more than the demand", %{pid: pid, conn: conn, tube: tube} do
      {:ok, stats} = Beanstix.stats_tube(conn, tube)
      assert stats["current-jobs-ready"] == 0

      jobs = for i <- 1..20, do: {:put, "Message #{i}"}
      Beanstix.pipeline(conn, jobs)

      for i <- 1..20 do
        msg = "Message #{i}"
        assert_receive {:message_handled, ^msg, _}
      end

      {:ok, stats} = Beanstix.stats_tube(conn, tube)
      assert stats["current-jobs-ready"] == 0

      jobs = for i <- 21..30, do: {:put, "Failed Message #{i}"}
      Beanstix.pipeline(conn, jobs)

      for i <- 21..30 do
        msg = "Failed Message #{i}"
        assert_receive {:message_handled, ^msg, _}
      end

      Process.sleep(100)

      {:ok, stats} = Beanstix.stats_tube(conn, tube)
      assert stats["current-jobs-delayed"] == 10

      cleanup(pid, conn, tube)
    end

    test "keep trying to receive new messages when the queue is empty", %{pid: pid, conn: conn, tube: tube} do
      Beanstix.put(conn, "Message 13")

      assert_receive {:message_handled, "Message 13", _}

      refute_receive {:message_handled, _, _}

      Beanstix.put(conn, "Message 14")
      Beanstix.put(conn, "Message 15")
      assert_receive {:message_handled, "Message 14", _}
      assert_receive {:message_handled, "Message 15", _}

      cleanup(pid, conn, tube)
    end

    test "delete acknowledged messages", %{pid: pid, conn: conn, tube: tube} do
      {:ok, stats} = Beanstix.stats_tube(conn, tube)
      assert stats["current-jobs-ready"] == 0

      jobs = for i <- 1..20, do: {:put, "Message #{i}"}
      Beanstix.pipeline(conn, jobs)

      Process.sleep(100)

      {:ok, stats} = Beanstix.stats_tube(conn, tube)
      assert stats["current-jobs-ready"] == 0

      cleanup(pid, conn, tube)
    end
  end

  defp start_broadway(tube) do
    Broadway.start_link(Forwarder,
      name: new_unique_name(),
      context: %{test_pid: self()},
      producers: [
        default: [
          module: {OffBroadwayBeanstalkd.Producer, receive_interval: 10, tube: tube},
          stages: 1
        ]
      ],
      processors: [
        default: [stages: 1]
      ]
    )
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  def tube_name() do
    {m, s, ms} = :os.timestamp()
    num = System.unique_integer([:positive, :monotonic])
    "BeanstixClient_#{m}_#{s}_#{ms}_#{num}"
  end

  defp cleanup(pid, conn, tube) do
    Beanstix.purge_tube(conn, tube)
    Beanstix.quit(conn)
    stop_broadway(pid)
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
