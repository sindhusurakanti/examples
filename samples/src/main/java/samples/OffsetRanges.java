package samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

public class OffsetRanges {

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		// Properties props = new Properties();

		Map<String, String> props = new HashMap<>();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("stream");

		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(4));
		System.out.println("started");

		Map<TopicAndPartition, Long> map = new HashMap<>();
		map.put(new TopicAndPartition("stream", 0), 1L);

		try {

			JavaInputDStream<Map.Entry> input = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class,
					StringDecoder.class, Map.Entry.class, props, map, messageAndMetadata -> new AbstractMap.SimpleEntry<>(
							messageAndMetadata.offset(), messageAndMetadata.message()));

			final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

			input.foreachRDD(rdd -> {
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);

				rdd.foreach(rec -> {
					for (OffsetRange s : offsetRanges.get()) {
						System.out.println(s.topic() + " " + s.partition() + " " + s.fromOffset() + " " + s.untilOffset());

						Class.forName("com.mysql.jdbc.Driver");
						Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/sindhu_spark", "root", "Tcs@1234");
						java.sql.Statement stmt = con.createStatement();

						String query = "insert into streamdata values('" + s.fromOffset() + "')";

						System.out.println("offset value " + s.fromOffset());
						stmt.executeUpdate(query);

						con.close();
					}
				});
			});
		} catch (Exception e) {
			e.printStackTrace();
		}

		jsc.start();
		jsc.awaitTermination();
	}

}
