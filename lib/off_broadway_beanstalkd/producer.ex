defmodule OffBroadwayBeanstalkd.Producer do
  @moduledoc """
  A GenStage producer that continuously polls messages from a beanstalkd queue and
  acknowledge them after being successfully processed.

  By default this producer uses `OffBroadwayBeanstalkd.BeanstixClient` to talk to beanstalkd but
  you can provide your client by implementing the `OffBroadwayBeanstalkd.BeanstalkdClient`
  behaviour.

  ## Options for `OffBroadwayBeanstalkd.BeanstixClient`

    * `:host` - The host beanstalkd is running on, default is '127.0.0.1'

    * `:port` - The port beanstalkd is running on, default is 11300

    * `:tube` - The name of the tube.

  ## Producer Options

  These options applies to all producers, regardless of client implementation:

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 1000.

    * `:beanstalkd_client` - Optional. A module that implements the `OffBroadwayBeanstalkd.BeanstalkdClient`
      behaviour. This module is responsible for fetching and acknowledging the
      messages. Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs. Default
      is `OffBroadwayBeanstalkd.BeanstixClient`.

  ## Acknowledgments

  In case of successful processing, the message is deleted from the queue.
  In case of failures, the message is released back to the ready queue.

  ### Batching

  Bathcing with Broadway is done using the `handle_batch/3` callback

  ## Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producers: [
          default: [
            module:
              {OffBroadwayBeanstalkd.Producer,
              host: "192.168.0.10",
              port: 11300,
              tube: "my_queue",
              requeue: :once
            stages: 5
          ]
        ],
        processors: [
          default: []
        ]
      )

  The above configuration will set up a producer that continuously receives
  messages from `"my_queue"` and sends them downstream.

  ## Retrieving Metadata

  By default the following information is added to the `metadata` field in the
  `%Message{}` struct:

    * `message_id` - The message id received when the message was sent to the queue
    * `receipt_handle` - The receipt handle
    * `md5_of_body` - An MD5 digest of the message body

  You can access any of that information directly while processing the message:

      def handle_message(_, message, _) do
        receipt = %{
          id: message.metadata.message_id,
          receipt_handle: message.metadata.receipt_handle
        }

        # Do something with the receipt
      end

  If you want to retrieve `attributes` or `message_attributes`, you need to
  configure the `:attributes_names` and `:message_attributes_names` options
  accordingly, otherwise, attributes will not be attached to the response and
  will not be available in the `metadata` field

      producers: [
        default: [
          module: {OffBroadwayBeanstalkd.Producer,
            queue_name: "my_queue",
            # Define which attributes/message_attributes you want to be attached
            attribute_names: [:approximate_receive_count],
            message_attribute_names: ["SomeAttribute"],
          }
        ]
      ]

  and then in `handle_message`:

      def handle_message(_, message, _) do
        approximate_receive_count = message.metadata.attributes["approximate_receive_count"]
        some_attribute = message.metadata.message_attributes["SomeAttribute"]

        # Do something with the attributes
      end

  For more information on the `:attributes_names` and `:message_attributes_names`
  options.
  """

  use GenStage

  @default_receive_interval 1000

  @impl true
  def init(opts) do
    client = opts[:beanstalkd_client] || OffBroadwayBeanstalkd.BeanstixClient
    receive_interval = opts[:receive_interval] || @default_receive_interval

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, opts} ->
        {:producer,
         %{
           demand: 0,
           receive_timer: nil,
           receive_interval: receive_interval,
           beanstalkd_client: {client, opts}
         }}
    end
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    messages = receive_messages_from_beanstalkd(state, demand)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_messages_from_beanstalkd(state, total_demand) do
    %{beanstalkd_client: {client, opts}} = state
    client.receive_messages(total_demand, opts)
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
