package samples;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

public class DatasetToDBNewRec {

	public static Dataset<Row> set;

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException, FileNotFoundException {
		// TODO Auto-generated method stub

		Map<String, String> props = new HashMap<>();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "2000");
		props.put("session.timeout.ms", "40000");
		props.put("Timestamp", "true");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Hello-Kafka");

		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(4));

		System.out.println("started");

		Map<TopicAndPartition, Long> map = new HashMap<>();
		map.put(new TopicAndPartition("CF_JAN_INTGR_FEB13", 0), 0L);

		data();
		set.show(120, false);
		try {
			JavaInputDStream<Map.Entry> input = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class,
					StringDecoder.class, Map.Entry.class, props, map, messageAndMetadata -> new AbstractMap.SimpleEntry<>(
							messageAndMetadata.offset(), messageAndMetadata.message()));

			input.foreachRDD(rdd ->

			{
				rdd.foreach(

				rec -> {
					Class.forName("com.mysql.jdbc.Driver");
					Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/sindhu_spark",
							"root", "root");
					java.sql.Statement stmt = con.createStatement();

					int val = Integer.parseInt(set.collect().toString());
					int dbVal = Integer.parseInt(rec.getValue().toString());

					System.out.println("offset " + rec.getKey() + "    message    " + rec.getValue());

					if (val > dbVal) {
						String query1 = "insert into stream values('" + rec.getKey() + "'  , ' " + rec.getValue() + "'  )";

						stmt.executeUpdate(query1);

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

	public static Dataset<Row> data() throws ClassNotFoundException {
		SparkSession spark = SparkSession.builder().appName(" SQL  example").config("conf", "-").getOrCreate();

		SQLContext sqc = new SQLContext(spark);

		Class.forName("com.mysql.jdbc.Driver");

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "root");

		Dataset<Row> s = sqc.read().jdbc("jdbc:mysql://localhost:3306/sindhu_spark", "tab",
				connectionProperties);

		s.createOrReplaceTempView("stream");

		return set = sqc.sql("select max(pay_trnst_msg_id) from stream");
	}

}
