import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

public class SupplierDeserializer implements Deserializer<Supplier> {
  private String encoding = "UTF8";

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public Supplier deserialize(String topic, byte[] data) {
    try {
      if (data == null) {
        return null;
      }
      ByteBuffer byteBuffer = ByteBuffer.wrap(data);
      int id = byteBuffer.getInt();
      int sizeOfName = byteBuffer.getInt();
      byte[] nameBytes = new byte[sizeOfName];
      byteBuffer.get(nameBytes);
      String deserializedName = new String(nameBytes, encoding);

      int sizeOfDate = byteBuffer.getInt();
      byte[] dateBytes = new byte[sizeOfDate];
      byteBuffer.get(dateBytes);
      String dateString = new String(dateBytes, encoding);

      DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
      return new Supplier(id, deserializedName, dateFormat.parse(dateString));

    } catch (Exception exception) {
      throw new SerializationException("Error occurred during deserialising");
    }
  }


  @Override
  public void close() {

  }
}
