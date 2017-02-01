import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by kecso on 1/31/17.
 */
public class SimpleConsumer {
    public static void main(String[] args) {
        System.out.println("Simple Consumer for Kafka");
        String topicName = "topic1";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "testGroup");
        // to sending auto ack to server
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timoute.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));

        System.out.println("Message is received from " + topicName);
        int i = 0;
        // just receive 10 messages
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> r: records) {
                System.out.println(r.toString());
                // TODO: check this - something oiing on here... why I get null for this: Integer.getInteger(r.key())
//                try {
//                    i = Integer.getInteger(""+r.key());
//                    System.out.println(r.key().getClass());
//                } catch (NullPointerException e) {
//                    System.out.println(r.toString());
//                    System.out.println(r.key());
//                    System.out.println(Integer.getInteger(r.key()));
//                    System.out.println(e.toString());
//                }
//                System.out.println("key: " + r.key());
            }
//            System.out.println("Last key: " + i);
        }
    }
}
