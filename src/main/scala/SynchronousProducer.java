import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SynchronousProducer {
  public static void main(String args[]) throws ExecutionException, InterruptedException {
    String topicName = "test2";
    String key = "key1";
    String value = "value1";

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(properties);
    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
    try {
      RecordMetadata recordMetadata = producer.send(record).get();
      System.out.println("Message is sent to partition number " + recordMetadata.partition() + " and offset " + recordMetadata.offset());
      System.out.println("Synchronous producer completed with success");
    } catch (Exception exception) {
      exception.printStackTrace();
      System.out.println("Synchronous producer failed with an exception");
    } finally {
      producer.close();
    }
  }
}
