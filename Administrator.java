import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/*** Administrator dostaje kopię wszystkich wiadomości przesyłanych w systemie oraz ma możliwość wysłania wiadomości w trzech trybach:
 - do wszystkich Agencji
 - do wszystkich Przewoźników
 - do wszystkich Agencji oraz Przewoźników
 ***/

public class Administrator {
    public static String BROADCAST_EXCHANGE_TOPIC = "broadcastExchangeTopic";
    public static String COPY_EXCHANGE_TOPIC = "copyExchangeTopic";
    public static String COPY_ROUTING_KEY = "copy";
    public static String ADMINISTRATOR = "[A]: Administrator";
    private static String messageHeader = ADMINISTRATOR + " sends message to ";
    private static ConnectionFactory factory;
    private static Connection connection;
    private static Channel channel;

    public static void main(String[] argv) throws Exception {
        // info
        System.out.println("ADMINISTRATOR");

        // connection & channel
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        try {
            channel.exchangeDeclare(BROADCAST_EXCHANGE_TOPIC, BuiltinExchangeType.TOPIC);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        channel.exchangeDeclare(COPY_EXCHANGE_TOPIC, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, COPY_EXCHANGE_TOPIC, COPY_ROUTING_KEY);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println("RECEIVED COPY OF: " + message);
            }
        };
        channel.basicConsume(queueName, true, consumer);

        while (true) {
            sendMessage();
        }
    }

    private static String getJobMessage(String broadcastQueueName, String message) {
        return messageHeader + broadcastQueueName + ": " + message;
    }

    private static void sendMessage() throws IOException {
        System.out.println("Type X to send Y: \n- 1 - only agencies \n- 2 - only carriers \n- 3 - all agencies and carriers...");

        int type = Integer.parseInt(MessageReadUtil.readMessage());
        String routingKey = Mode.getRoutingKey(type);

        System.out.println("Type message: ");
        String message = MessageReadUtil.readMessage();

        String requestMessage = getJobMessage(routingKey, message);
        System.out.println("ROUTING KEY: " + routingKey);
        channel.basicPublish(BROADCAST_EXCHANGE_TOPIC, routingKey, null, requestMessage.getBytes(StandardCharsets.UTF_8));
        System.out.println("SENT: " + requestMessage);
    }
}

enum Mode {
    ALL_AGENCIES(1, "agencies"), ALL_CARRIERS(2, "carriers"), ALL_AGENCIES_AND_CARRIERS(3, "everyone");

    int type;
    String routingKey;

    Mode(int type, String routingKey) {
        this.type = type;
        this.routingKey = routingKey;
    }

    public static String getRoutingKey(int type) {
        for (Mode mode : Mode.values()) {
            if (mode.type == type) {
                return mode.routingKey;
            }
        }
        throw new IllegalArgumentException();
    }
}
