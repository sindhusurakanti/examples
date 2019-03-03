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
import java.util.List;
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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Function;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;





import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.spark.streaming.receiver.Receiver;















import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import scala.Tuple2;import scala.util.parsing.json.JSONObject;

public class Test  {

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
		
		 SparkSession spark = SparkSession
				  .builder()
				  .appName(" SQL  example")
				  .config("conf", "-")
				  .getOrCreate();

				SQLContext sqc=new SQLContext(spark);

		
		 System.out.println("started");
		 
		Map<TopicAndPartition,Long> map=new HashMap<>();
		map.put(new TopicAndPartition("Hello-Kafka",0),1L);
		
		
		StructType schema = new StructType(
		        new StructField[] {
		            DataTypes.createStructField("id", DataTypes.StringType, true) ,
		            DataTypes.createStructField("name", DataTypes.StringType, true)
		        } );

	    JavaPairInputDStream<String, String> directKafkaStream = 
	            KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class,
	                    StringDecoder.class, props, topic);

	   
	    directKafkaStream.foreachRDD(rdd -> {
	        rdd.foreach(record -> 
	        {
	         	System.out.println("recccc"+record);
	         	
				JsonParser p=new JsonParser();

	         	
				

	         	
	         	    	Tuple2<String, String> rowRDD =  record;
	         	
	         	List<Row> row=(List<Row>) rowRDD;
	         	
	  /*       	
	         	
	         	System.out.println(frame);
	         	
	         JsonParser p=new JsonParser();
	         	
	         	p.parse(record.toString());
	         	
	         	List<Row> r=(List<Row>) record;
	         	
	         	Reader r1=record;
	         	
	         JsonReader read=new JsonReader(   r1  );
	         	System.out.println(p);     
	         	
*/
				
	         	
	        }
	       	        		);
	       
	    });
	    
	//    RDD<String> s= (RDD<String>)directKafkaStream;
	 //   Dataset<Row> r=sqc.read().json(s);
	    
	   	    jsc.start();
	    jsc.awaitTermination();
	}

}
