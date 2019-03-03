package samples;

import java.sql.Connection;
import java.io.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Function;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;





import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.spark.streaming.receiver.Receiver;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;

import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.mutable.ListBuffer;
import scala.util.parsing.json.JSONArray;
import scala.util.parsing.json.JSONObject;

public class Json  {

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
	      String topicName="Hello-Kafka3";
	      
		Set<String> topic=new HashSet<>(Arrays.asList(topicName));
		SparkConf sc =new SparkConf().setMaster("local[*]").setAppName("Hello-Kafka3");
		
		JavaStreamingContext  jsc =new JavaStreamingContext(sc,Durations.seconds(4));
		 System.out.println("started");
		 
		Map<TopicAndPartition,Long> map=new HashMap<>();
		map.put(new TopicAndPartition( "Hello-Kafka3",0),1L);
		
				
				StructType schema = new StructType(
				        new StructField[] {
				            DataTypes.createStructField("initialpay", DataTypes.StringType, true) ,
				            DataTypes.createStructField("bidmonth", DataTypes.StringType, true)
				        } );	
		
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
		 
		
	/*	 
		 input.foreachRDD(rdd->
		 			
				 {	
					 rdd.foreach( 
							 new VoidFunction<Tuple2<String,String>>() {

								@Override
								public void call(Tuple2<String, String> rec)
										throws Exception {
									// TODO Auto-generated method stub
									   
									String message=rec._2;
									
								}

								
										
									   
								
								 
							}
							 );
					
						  
					
					 
				 }  );

		*/
			jsc.start();
	jsc.awaitTermination();
	}

}
