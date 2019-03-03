package samples;

import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;



















import kafka.common.TopicAndPartition;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;



















import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.Configurator;















import org.codehaus.jackson.map.ObjectMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.mysql.jdbc.Statement;



















import scala.Tuple2;import scala.util.parsing.json.JSONObject;


public class Offset {

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException  {
		// TODO Auto-generated method stub   
		
		
		org.apache.log4j.Logger log = Logger.getLogger(Offset.class.getName());		
		 Map<String,String> props= new HashMap<>();
	      props.put("bootstrap.servers", "localhost:9092");
	      props.put("group.id", "test");
	      
	      props.put("enable.auto.commit", "true");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      String topicName="Hello-Kafka3";
	      
		//Collection<String> topic=Arrays.asList("stream");
		SparkConf sc =new SparkConf().setMaster("local[*]").setAppName("stream");
		
		
		JavaStreamingContext  jsc =new JavaStreamingContext(sc,Durations.seconds(4));
		log.trace(" my Trace Message!");
		System.out.println(jsc);
		Set<String> topic=new HashSet<>(Arrays.asList(topicName));
		
		Map<TopicAndPartition,Long> map=new HashMap<>();
		map.put(new TopicAndPartition("Hello-Kafka3",0),1L);
		  
		 final AtomicReference<OffsetRange[]> range= new AtomicReference<>();
		          
		System.out.println(range);
		 
		log.warn(" my warning");
		
		JavaPairInputDStream<String, String> Stream = KafkaUtils.createDirectStream( jsc,String.class, String.class, StringDecoder.class,
				StringDecoder.class,  props, topic );

		
	
		
		
		
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
       
	
		log.warn("connection stmt");
		
        Stream.transformToPair(
                (Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>) rdd -> {
                    OffsetRange[] offsets = ( (HasOffsetRanges) rdd.rdd() ).offsetRanges();
                    offsetRanges.set(offsets);
                    return rdd;
                    
                }
        ).foreachRDD(
                (VoidFunction<JavaPairRDD<String, String>>) stringStringJavaPairRDD -> {
                    for (OffsetRange s: offsetRanges.get()) {
                        System.out.println(
                               //log.debug( 
                        		s.topic() + " " + s.partition() + " " + s.fromOffset() + " " + s.untilOffset() );
                   //     );
                        Class.forName("com.mysql.jdbc.Driver");
                		Connection con =DriverManager.getConnection("jdbc:mysql://localhost:3306/sindhu_spark", "root","Tcs@1234" );
                		java.sql.Statement stmt=con.createStatement();
                        
                      String query="insert into streamdata values( ' "+s.fromOffset()+ " '  )";
                      
                      stmt.executeUpdate(query);
                      log.error("table not found");
                      log.error("columns not found");
                        		
                        log.info("inserting into db");
                       
                                            }
                }
        );
        
		
    	
      //  LogManager.shutdown();
		jsc.start();
		jsc.awaitTermination();
		
	}

}
