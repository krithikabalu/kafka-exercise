import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SensorProducer {
  public static void main(String args[]) throws ExecutionException, InterruptedException {
    String topicName = "SensorTopic";

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("partitioner.class", "SensorPartitioner");
    properties.put("speed.sensor.name", "TSS");

    Producer<String, String> producer = new KafkaProducer<>(properties);
    try {
      for (int i = 0; i < 10; i++) {
        RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topicName, "SSP" + i, "500" + i)).get();
        System.out.println("partition " + recordMetadata.partition());
        System.out.println("offset" + recordMetadata.offset());
      }
      for (int i = 0; i < 10; i++) {
        RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topicName, "TSS", "500" + i)).get();
        System.out.println("partition " + recordMetadata.partition());
        System.out.println("offset" + recordMetadata.offset());
      }
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    producer.close();
  }
}
