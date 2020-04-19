import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/***
 Agencje kosmiczne zlecają wykonanie trzech typów usług: przewóz osób, przewóz ładunku, umieszczenie satelity na orbicie.
 ***/

public class SpaceAgency {
    private static String agencyName;
    public static String RESPONSE_EXCHANGE_DIRECT = "responseExchangeDirect";
    private static int jobNumber = 0;
    private static List<String> queueNames = new ArrayList<>();
    private static Channel channel;
    private static Connection connection;
    private static ConnectionFactory factory;

    public static void main(String[] argv) throws Exception {
        fetchAgencyName();

        // connection & channel
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(Administrator.BROADCAST_EXCHANGE_TOPIC, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(RESPONSE_EXCHANGE_DIRECT, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(Administrator.COPY_EXCHANGE_TOPIC, BuiltinExchangeType.DIRECT);

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, Administrator.BROADCAST_EXCHANGE_TOPIC, Mode.ALL_AGENCIES.routingKey);
        channel.queueBind(queueName, RESPONSE_EXCHANGE_DIRECT, agencyName);

//        it should have been done with simply setting routing key to * or #, or something similar, but any of these worked
        channel.queueBind(queueName, Administrator.BROADCAST_EXCHANGE_TOPIC, Mode.ALL_AGENCIES_AND_CARRIERS.routingKey);



        // declare queues
        queueNames = Arrays.stream(ServiceType.values()).map(q -> q.name).collect(Collectors.toList());
        queueNames.forEach(qn -> {
            try {
                channel.queueDeclare(qn, false, false, false, null);
            } catch (IOException e) {
                System.err.println("IOException");
            }
        });
        channel.basicQos(1); // accept only one unack-ed message at a time

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println("RECEIVED: " + message);
            }
        };

        channel.basicConsume(queueName, true, consumer);

        while (true) {
            System.out.println("Type X to send Y: \n- 1 - people \n- 2 - load \n- 3 - satellite...");
            ServiceType serviceType = getServiceType();
            String requestMessage = getJobMessage(serviceType.name);
            channel.basicPublish("", serviceType.name, null, requestMessage.getBytes());
            channel.basicPublish(Administrator.COPY_EXCHANGE_TOPIC, Administrator.COPY_ROUTING_KEY, null, requestMessage.getBytes(StandardCharsets.UTF_8));
            System.out.println("SENT: " + requestMessage);
            jobNumber++;
        }
    }

    private static void fetchAgencyName() throws IOException {
        System.out.println("Type SPACE AGENCY's name: ");
        agencyName = MessageReadUtil.readMessage();
        System.out.println("SPACE AGENCY: " + agencyName);
    }

    private static String getJobMessage(String serviceName) {
        return agencyName + " jobNo: " + jobNumber + " service type's name: " + serviceName;
    }

    private static ServiceType getServiceType() throws IOException {
        return ServiceType.of(new Integer(MessageReadUtil.readMessage().trim()));
    }
}
