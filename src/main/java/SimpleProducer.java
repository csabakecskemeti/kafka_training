import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by kecso on 1/30/17.
 */
public class SimpleProducer {
    public static void main(String[] args) {
        System.out.println("Simple Producer for Kafka");
        String topicName = "topic1";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        // producer want ack for all messages
        properties.put("acks", "all");
        // infinite retry
        properties.put("retries", 0);
        // 16kb buffer
        properties.put("buffer.size", 16324);
        // messages sent in 1ms chunks
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), "message: " + i));
            System.out.println("msg sent");
        }
        producer.close();
    }
}
