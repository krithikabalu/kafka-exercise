import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class SupplierConsumer {
  public static void main(String args[]) {
    String topicName = "SupplierTopic";
    String groupName = "SupplierTopicGroup";

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
    properties.put("group.id", groupName);
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "SupplierDeserializer");

    KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topicName));
    while (true) {
      ConsumerRecords<String, Supplier> records = consumer.poll(100);
      for (ConsumerRecord<String, Supplier> record : records) {
        System.out.println("Supplier id= " + String.valueOf(record.value().getID()) +
            " Supplier  Name = " + record.value().getName() +
            " Supplier Start Date = " + record.value().getStartDate().toString() +
            "Key "+record.key());

      }
    }
  }
}
