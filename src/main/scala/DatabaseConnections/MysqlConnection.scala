package DatabaseConnections

import java.util.Properties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MysqlConnection {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR) // Display the logs only if have errors

    //Creating spark session
    val spark = SparkSession.builder()
      .appName("Mysql Database connection")
      .master("local[*]") // Change master to yarn if you running in cluster
      .getOrCreate()
    //Necessary parameters creation

    val url = "jdbc:mysql://localhost:3306"
    val tableName = "world.country"
    val props = new Properties()
    props.put("user", "venkat") //Specify the db user name & Password
    props.put("password", "root")

    // Driver class
    Class.forName("com.mysql.jdbc.Driver")

    //DataFrame creation

    val mysqlDF = spark.read.jdbc(url, tableName, props)

    mysqlDF.show(30)

  }

}
