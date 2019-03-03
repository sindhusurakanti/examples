package samples;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Joins {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
		// TODO Auto-generated method stub


		SparkSession spark = SparkSession.builder().appName(" SQL  example").config("conf", "-").getOrCreate();

		SQLContext sqc = new SQLContext(spark);

		Class.forName("com.mysql.jdbc.Driver");
		String MYSQL_USERNAME = "root";
		String MYSQL_PWD = "Tcs@1234";

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", MYSQL_USERNAME);
		connectionProperties.put("password", MYSQL_PWD);

		Dataset<Row> s = sqc.read().jdbc("jdbc:mysql://localhost:3306/sindhu_spark", "sindhu_spark.table1", connectionProperties);

		s.createOrReplaceTempView("table1");

		Dataset<Row> s1 = sqc.read().jdbc("jdbc:mysql://localhost:3306/sindhu_spark", "sindhu_spark.table2", connectionProperties);

		s1.createOrReplaceTempView("table2");

		Dataset<Row> globalrdd = sqc.sql("select * from  table1 inner join table2 on table1.fname=table2.fname ");

		globalrdd.show(10, false);

		Dataset<Row> set2 = sqc.sql("select * from table2 right join table1 on table2.empid=table1.empid");

		set2.show(10, false);

		globalrdd.filter("empid >12").show();

	}

}
