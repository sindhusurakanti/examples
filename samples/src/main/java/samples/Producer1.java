package samples;

import java.io.FileReader;
import java.util.Properties;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

//Create java class named “SimpleProducer”
public class Producer1 {

	public static void main(String[] args) throws Exception {

		// Check arguments length value

		// Assign topicName to string variable
		// String topic = "Hello-Kafka" ;

		String topicName = "stream";

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");

		props.put("acks", "all");

		props.put("retries", 0);

		props.put("batch.size", 16384);

		props.put("linger.ms", 1);

		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		JsonParser parser = new JsonParser();

		JsonElement obj = parser.parse(new FileReader("/root/253126/sample1.txt"));

		String abc = obj.toString();
		System.out.println("msg;;;;;;;" + abc);
		producer.send(new ProducerRecord<String, String>(topicName, abc, abc));
		System.out.println("Message sent successfully");

		producer.close();
	}
}