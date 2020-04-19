import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/*** Administrator dostaje kopię wszystkich wiadomości przesyłanych w systemie oraz ma możliwość wysłania wiadomości w trzech trybach:
 - do wszystkich Agencji
 - do wszystkich Przewoźników
 - do wszystkich Agencji oraz Przewoźników
 ***/

public class Administrator {
    public static String BROADCAST_EXCHANGE_TOPIC = "broadcastExchangeTopic";
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

        //TODO: test
        // declare broadcast queues
        Arrays.stream(Mode.values()).forEach(mode ->
        {
            try {
                channel.exchangeDeclare(BROADCAST_EXCHANGE_TOPIC, BuiltinExchangeType.TOPIC);
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        });

        // TODO:            channel.basicConsume(agencyName, false, consumer);

        while (true) {
            sendMessage();
//            todo: try to receive a copy of each message
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
    ALL_AGENCIES(1, "agencies"), ALL_CARRIERS(2, "carriers"), ALL_AGENCIES_AND_CARRIERS(3, "*");

    int type;
    String routingKey;

    Mode(int type, String routingKey) {
        this.type = type;
        this.routingKey = routingKey;
    }

//    public Mode of(String queueName) {
//        for (Mode mode : Mode.values()) {
//            if (mode.queueName.equals(queueName)) {
//                return mode;
//            }
//        }
//        throw new IllegalArgumentException();
//    }

    public static String getRoutingKey(int type) {
        for (Mode mode : Mode.values()) {
            if (mode.type == type) {
                return mode.routingKey;
            }
        }
        throw new IllegalArgumentException();
    }
}
