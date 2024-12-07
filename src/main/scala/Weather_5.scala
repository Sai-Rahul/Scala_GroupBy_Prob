import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Weather_5 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Weather_5")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val weatherData = Seq(
      ("City1", "2022-01-01", 10.0),
      ("City1", "2022-01-02", 8.5),
      ("City1", "2022-01-03", 12.3),
      ("City2", "2022-01-01", 15.2),
      ("City2", "2022-01-02", 14.1),
      ("City2", "2022-01-03", 16.8)
    ).toDF("City", "Date", "Temperature")
    weatherData.show()
    import org.apache.spark.sql.functions._
    // Group by City and calculate the minimum, maximum, and average temperature
    val temperatureStatsByCity = weatherData.groupBy("City")
      .agg(
        min("Temperature").alias("MinTemperature"),
        max("Temperature").alias("MaxTemperature"),
        avg("Temperature").alias("AvgTemperature")
      )

    temperatureStatsByCity.show()


  }
}
