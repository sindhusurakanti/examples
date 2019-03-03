package samples;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class ParellelizeTopicData {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		Map<String, String> props = new HashMap<>();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		SparkConf sc = new SparkConf().setMaster("local[8]").setAppName("Hello-Kafka1");

		JavaSparkContext jsct = new JavaSparkContext(sc);
		JavaStreamingContext jsc = new JavaStreamingContext(jsct, Durations.seconds(4));

		System.out.println("started");

		Map<TopicAndPartition, Long> map = new HashMap<>();
		map.put(new TopicAndPartition("Hello-Kafka1", 0), 1L);

		SparkSession spark = SparkSession.builder().appName(" SQL  example").config("conf", "-").getOrCreate();

		SQLContext sqc = new SQLContext(spark);

		ArrayList<Map<String, String>> array = new ArrayList<Map<String, String>>();

		JavaDStream<Map.Entry> input = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class,
				Map.Entry.class, props, map,
				messageAndMetadata -> new AbstractMap.SimpleEntry<>(messageAndMetadata.offset(), messageAndMetadata.message())).repartition(8);

		input.foreachRDD(rdd ->

		{
			rdd.foreach(new VoidFunction<Entry>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Entry rec) throws Exception {
					// TODO Auto-generated method stub

					Map<String, String> map = new HashMap<String, String>();
					map.put(rec.getKey().toString(), rec.getValue().toString());
					array.add(map);
					System.out.println("map" + map);
				}
			});

			rdd.foreachPartition(row -> {
				System.out.println("process");
				Thread.sleep(1000);
			}

			);

		});
		JavaRDD<Map<String, String>> set = jsct.parallelize(array);

		set.foreach(record -> {
			Thread.sleep(1000);
		});

		Dataset<Row> df = spark.read().json("/root/253126/sample1.txt");

		Dataset<Row> s = sqc.sql(" select  * from sample1");
		Dataset<Row> sp = s.sort("month");
		sp.show(170, false);

		Dataset<Row> dfh = sqc.read().json("/root/253126/car.json").toDF();

		dfh.schema();

		df.drop("_corrupt_record");

		System.out.println("df " + df);

		jsc.start();
		jsc.awaitTermination();

	}

}
