package samples;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import kafka.common.TopicAndPartition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class JsonToDataset {

	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
		// TODO Auto-generated method stub

		System.out.println("started");

		Map<TopicAndPartition, Long> map = new HashMap<>();
		map.put(new TopicAndPartition("Hello-Kafka", 0), 1L);

		SparkSession spark = SparkSession.builder().appName(" SQL  example").config("conf", "-").getOrCreate();

		SQLContext sqc = new SQLContext(spark);

		Dataset<Row> row = sqc.read().json("/root/253126/cars1.json");

		row.select("*").join(row, "itemNo").show();

		System.out.print("orderbyyyyyy" + row.orderBy("speed").collectAsList());

		System.out.print("filterrrr" + row.filter("speed>300").collectAsList());

		System.out.print("sorttt" + row.sort("itemNo").collectAsList());

		 System.out.print("duplicatesss"+row.dropDuplicates().collectAsList());

	}

}
