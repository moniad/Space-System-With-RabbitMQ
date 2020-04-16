import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/***
Agencje kosmiczne zlecają wykonanie trzech typów usług: przewóz osób, przewóz ładunku, umieszczenie satelity na orbicie.
***/

public class SpaceAgency {
    private static String agencyName;
    private static int jobNumber = 0;
    private static List<String> queueNames = new ArrayList<>();

    public static void main(String[] argv) throws Exception {
        fetchAgencyName();

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

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

        while (true) {
            // start listening
            System.out.println("Type X to send Y: \n- 1 - people \n- 2 - load \n- 3 - satellite...");
            ServiceType serviceType = getServiceType();
            channel.basicPublish("", serviceType.name, null, getJobMessage());
            jobNumber++;
        }

        //        channel.basicConsume(QUEUE_NAME, false, consumer);


        // close
//        channel.close();
//        connection.close();
    }

    private static void fetchAgencyName() throws IOException {
        System.out.println("Type SPACE AGENCY's name: ");
        agencyName = MessageReadUtil.readMessage();
        System.out.println("SPACE AGENCY " + agencyName);
    }

    private static byte[] getJobMessage() {
        return (agencyName + " " + jobNumber).getBytes();
    }

    private static ServiceType getServiceType() throws IOException {
        return ServiceType.of(new Integer(MessageReadUtil.readMessage().trim()));
    }
}
