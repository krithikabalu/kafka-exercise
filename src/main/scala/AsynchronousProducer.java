import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AsynchronousProducer {
  public static void main(String args[]) {
    String topicName = "test2";
    String key = "key1";
    String value = "value1";

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(properties);
    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
    producer.send(record, new MyProducerCallback());
    producer.close();
  }
}
