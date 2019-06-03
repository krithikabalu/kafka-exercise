import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      System.out.println("Asynchronous producer failed");
    } else {
      System.out.println("Asynchronous success");
    }
  }
}
