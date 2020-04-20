import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/***
 - każdy Przewoźnik świadczy dokładnie 2 z 3 typów usług - przystępując do współpracy określa które 2 typy usług świadczy
 - konkretne zlecenie na wykonanie danej usługi powinno trafić do pierwszego wolnego Przewoźnika, który obsługuje ten typ zlecenia
 - dane zlecenie nie może trafić do więcej niż jednego Przewoźnika
 - zlecenia identyfikowane są przez nazwę Agencji oraz wewnętrzny numer zlecenia nadawany przez Agencję
 - po wykonaniu usługi Przewoźnik wysyła potwierdzenie do Agencji
 ***/

public class SpaceCarrier {
    private static List<ServiceType> serviceTypes;
    private static final String confirmationHeader = "[C]: Finished job: ";
    private static ConnectionFactory factory;
    private static Connection connection;
    private static Channel channel;

    public static void main(String[] argv) throws Exception {
        assignServiceTypes();

        // connection & channel
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(Administrator.BROADCAST_EXCHANGE_TOPIC, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(SpaceAgency.RESPONSE_EXCHANGE_DIRECT, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(Administrator.COPY_EXCHANGE_DIRECT, BuiltinExchangeType.DIRECT);

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, Administrator.BROADCAST_EXCHANGE_TOPIC, Mode.ALL_CARRIERS.routingKey);
        channel.queueBind(queueName, Administrator.BROADCAST_EXCHANGE_TOPIC, Mode.ALL_AGENCIES_AND_CARRIERS.routingKey); // not pretty

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println("RECEIVED: " + message);
                String responseRoutingKey = message.split(" ")[0]; // first string in request message is agency's or admin's name
                if (!responseRoutingKey.equals(Administrator.ADMINISTRATOR.split(" ")[0])) {
                    String confirmationMessage = confirmationHeader + message;
                    channel.basicPublish(SpaceAgency.RESPONSE_EXCHANGE_DIRECT, responseRoutingKey, null, confirmationMessage.getBytes(StandardCharsets.UTF_8));
                    channel.basicPublish(Administrator.COPY_EXCHANGE_DIRECT, Administrator.COPY_ROUTING_KEY, null, confirmationMessage.getBytes(StandardCharsets.UTF_8));
                    System.out.println("SENT: " + confirmationMessage);
                }
            }
        };

        // declare queues
        serviceTypes.stream().map(q -> q.name).forEach(qn -> {
            try {
                channel.queueDeclare(qn, false, false, false, null);
                channel.basicConsume(qn, true, consumer);
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        });

        System.out.println("Waiting for jobs...");
        channel.basicConsume(queueName, true, consumer);
    }

    private static void assignServiceTypes() throws IOException {
        System.out.println("Which two types of service will this carrier provide? [Number1,Number2]");
        serviceTypes = Stream.of(MessageReadUtil.readMessage().split(",")).limit(2).map(num -> ServiceType.of(new Integer(num))).collect(Collectors.toList());
        if (serviceTypes.size() < 2) {
            System.err.println("Incorrect number of services types");
            System.exit(1);
        }
        serviceTypes.stream().map(e -> e.name).forEach(System.out::println);
    }
}
