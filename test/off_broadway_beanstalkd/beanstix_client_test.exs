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

      assert message1.acknowledger == {BeanstixClient, opts.ack_ref, %{id: message1.metadata.job_id}}

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

      ack_data_1 = %{id: job_id1}
      ack_data_2 = %{id: job_id2}

      BeanstixClient.ack(
        opts.ack_ref,
        [
          %Message{acknowledger: {BeanstixClient, opts.ack_ref, ack_data_1}, data: nil},
          %Message{acknowledger: {BeanstixClient, opts.ack_ref, ack_data_2}, data: nil}
        ],
        []
      )

      Beanstix.purge_tube(opts.conn, opts.tube)
      Beanstix.quit(opts.conn)
    end
  end
end
