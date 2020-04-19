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
    private static final int timeToSleep = 10;
    private static final String confirmationHeader = "Finished job: ";

    public static void main(String[] argv) throws Exception {
        // info
        System.out.println("SPACE CARRIER");
        assignServiceTypes();

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println("Received: " + message);
                String confirmationMessage = confirmationHeader + message;
                String responseQueueName = message.split(" ")[0]; // first string in request message is agency's name
                channel.basicPublish("", responseQueueName, null, confirmationMessage.getBytes());
                channel.basicAck(envelope.getDeliveryTag(), false); // send ack
            }
        };

        // declare queues
        serviceTypes.stream().map(q -> q.name).forEach(qn -> {
            try {
                System.out.println(qn);
                channel.queueDeclare(qn, false, false, false, null);
                channel.basicConsume(qn, false, consumer);
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        });

        System.out.println("Waiting for jobs...");
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
