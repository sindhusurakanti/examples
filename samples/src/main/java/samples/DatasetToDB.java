package samples;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class DatasetToDB {

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		Map<String, String> props = new HashMap<>();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


		SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Hello-Kafka").set("spark.executor.instances", "2")
				.set("spark.cores.max", "6");

		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(4));
		System.out.println("started");

		SparkSession spark = SparkSession.builder().appName(" SQL  example").config("conf", "-").getOrCreate();

		SQLContext sqc = new SQLContext(spark);

		Map<TopicAndPartition, Long> map = new HashMap<>();
		map.put(new TopicAndPartition("Hello-Kafka", 0), 1L);

		JavaInputDStream<Map.Entry> input = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class,
				Map.Entry.class, // <--- This is the record return type from the
									// transformation.
				props, map, messageAndMetadata -> new AbstractMap.SimpleEntry<>(messageAndMetadata.offset(), messageAndMetadata.message()));

		input.foreachRDD((rdd, time) ->

		{

			rdd.foreach(rec -> {

				System.out.println("offset " + rec.getKey());
				System.out.println("message  " + rec.getValue());
				System.out.println("timestamp" + time);

				Class.forName("com.mysql.jdbc.Driver");
				
				String MYSQL_USERNAME = "root";
				String MYSQL_PWD = "Tcs@1234";

				Properties connectionProperties = new Properties();
				connectionProperties.put("user", MYSQL_USERNAME);
				connectionProperties.put("password", MYSQL_PWD);

				Dataset<Row> s = sqc.read().jdbc("jdbc:mysql://localhost:3306/sindhu_spark", "sindhu_spark.table1", connectionProperties);

				s.createOrReplaceTempView("table1");

				Dataset<Row> rdd1 = sqc.sql("select * from table1");

				rdd1.show(10, false);

			});
		});

		jsc.start();
		jsc.awaitTermination();

	}

}
