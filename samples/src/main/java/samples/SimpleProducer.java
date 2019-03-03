package samples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
















//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

//Create java class named “SimpleProducer”
public class SimpleProducer {
 
 public static void main(String[] args) throws Exception{
    
    // Check arguments length value
    
    
    //Assign topicName to string variable
	
String topicName = "Hello-Kafka1"  ;



    
    
    // create instance for properties to access producer configs   
    Properties props = new Properties();
    
    //Assign localhost id
    props.put("bootstrap.servers", "localhost:9092");
    
    //Set acknowledgements for producer requests.      
    props.put("acks", "all");
    
    //If the request fails, the producer can automatically retry,
    props.put("retries", 0);
    
    //Specify buffer size in config
    props.put("batch.size", 16384);
    
    //Reduce the no of requests less than 0   
    props.put("linger.ms", 1);
    
    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
    props.put("buffer.memory", 33554432);
    
    props.put("key.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
       
    props.put("value.serializer", 
       "org.apache.kafka.common.serialization.StringSerializer");
    
    Producer<String, String> producer = new KafkaProducer<String, String>(props);
          
    JsonParser parser=    new JsonParser();
    
   JsonElement obj =parser.parse(new FileReader("/root/253126/sample1.txt"));
    
  
    
    //FileReader file=new FileReader("/root/253126/sample1.txt");
    
    //BufferedReader buffer=new  BufferedReader(obj);
    
    String abc =obj.toString();
    System.out.println("msg;;;;;;;"+abc);
       producer.send(new ProducerRecord<String, String>(topicName , abc, abc));
             System.out.println("Message sent successfully");
             producer.close();
 }
}