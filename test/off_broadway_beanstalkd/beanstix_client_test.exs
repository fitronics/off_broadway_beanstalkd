defmodule OffBroadwayBeanstalkd.BeanstixClientTest do
  use ExUnit.Case

  alias Broadway.Message
  alias OffBroadwayBeanstalkd.BeanstixClient

  describe "receive_messages/2" do
    setup do
      {m, s, ms} = :os.timestamp()
      tube = "BeanstixClient_#{m}_#{s}_#{ms}"
      %{opts: [tube: tube]}
    end

    test "returns a list of Broadway.Message with :data and :acknowledger set", %{opts: base_opts} do
      {:ok, opts} = BeanstixClient.init(base_opts)
      Beanstix.pipeline(opts.conn, [{:put, "Message 1"}, {:put, "Message 2"}])

      [message1, message2] = BeanstixClient.receive_messages(10, opts)

      assert message1.data == "Message 1"
      assert message2.data == "Message 2"

      assert message1.acknowledger == {BeanstixClient, opts.ack_ref, %{job_id: message1.metadata.job_id}}

      Beanstix.purge_tube(opts.conn, opts.tube)
      Beanstix.quit(opts.conn)
    end

    test "add job_id to metadata", %{opts: base_opts} do
      {:ok, opts} = BeanstixClient.init(base_opts)

      [{:ok, job_id1}, _] = Beanstix.pipeline(opts.conn, [{:put, "Message 1"}, {:put, "Message 2"}])

      [%{metadata: metadata} | _] = BeanstixClient.receive_messages(10, opts)

      assert metadata.job_id == job_id1

      Beanstix.purge_tube(opts.conn, opts.tube)
      Beanstix.quit(opts.conn)
    end
  end

  describe "ack/2" do
    setup do
      {m, s, ms} = :os.timestamp()
      tube = "BeanstixClient_#{m}_#{s}_#{ms}"
      %{opts: [tube: tube]}
    end

    test "send and receive", %{opts: base_opts} do
      {:ok, opts} = BeanstixClient.init(base_opts)
      [{:ok, job_id1}, {:ok, job_id2}] = Beanstix.pipeline(opts.conn, [{:put, "Message 1"}, {:put, "Message 2"}])

      metadata_1 = %{job_id: job_id1, requeue: :never}
      metadata_2 = %{job_id: job_id2, requeue: :never}

      BeanstixClient.ack(
        opts.ack_ref,
        [
          %Message{acknowledger: nil, data: nil, metadata: metadata_1},
          %Message{acknowledger: nil, data: nil, metadata: metadata_2}
        ],
        []
      )

      Beanstix.purge_tube(opts.conn, opts.tube)
      Beanstix.quit(opts.conn)
    end
  end
end
