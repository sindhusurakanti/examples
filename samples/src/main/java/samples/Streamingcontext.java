package samples;

import java.sql.Connection;
import java.io.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.catalog.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;





import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.spark.streaming.receiver.Receiver;

import scala.Tuple2;


public class Streamingcontext  {

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		 //Properties props = new Properties();
		
		
		
	      
		 Map<String,String> props= new HashMap<>();
	      props.put("bootstrap.servers", "localhost:9092");
	      props.put("group.id", "test");
	      props.put("enable.auto.commit", "false");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      String topicName="Hello-Kafka";
	      
		Set<String> topic=new HashSet<>(Arrays.asList(topicName));
		SparkConf sc =new SparkConf().setMaster("local[*]").setAppName("stream");
		
		JavaStreamingContext  jsc =new JavaStreamingContext(sc,Durations.seconds(4));
		 System.out.println("started");
		 
		Map<TopicAndPartition,Long> map=new HashMap<>();
		map.put(new TopicAndPartition("Hello-Kafka",0),1L);
		
		try
		 {
		
		 JavaInputDStream<Map.Entry> input = KafkaUtils.createDirectStream(
		           jsc,
		           String.class,
		           String.class,
		           StringDecoder.class,
		           StringDecoder.class,
		           Map.Entry.class, // <--- This is the record return type from the transformation.
		           props,
		           map,
		         messageAndMetadata -> 
		           new AbstractMap.SimpleEntry<>(messageAndMetadata.offset(), messageAndMetadata.message()  )   );
		 
		 input.foreachRDD(rdd->
		 			
				 {
					 rdd.foreach( rec  ->  {
						 
						 
					System.out.println( "offset "+rec.getKey()     );
					 System.out.println("message  "+rec.getValue()  );
					 
		/*			 Class.forName("com.mysql.jdbc.Driver");
						Connection con =DriverManager.getConnection("jdbc:mysql://localhost:3306/sindhu_spark", "root","Tcs@1234" );
						java.sql.Statement stmt=con.createStatement();
						
						 String first ="select max(offset) from stream" ;
						
				ResultSet  out = stmt.executeQuery(first);
				out.next();
				String s1=rec.getKey().toString();
				
				 int off=Integer.parseInt(s1);
				 
				 int offset_db=out.getInt(1);
				
				if(offset_db  < off)
				{
						String query=" insert into stream  values ( '"+rec.getKey()+"' ,  ' " +rec.getValue()+"')      ";
						System.out.println("streaming....................");
						stmt.executeUpdate(query);
				}   
					 con.close();
							
				*/
					 } 	  );
				 }  );
		 
		 }
		 catch(Exception e)
			{
				e.printStackTrace(); 
			}
				
			jsc.start();
	jsc.awaitTermination();
	}

}
