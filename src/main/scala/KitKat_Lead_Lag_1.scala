import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object KitKat_Lead_Lag_1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "KitKat_Lead_Lag_1")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val kitkatPrice = List(
      (1, "Kitkat", 1000, "2021-01-01"),
      (2, "Kitkat", 2000, "2021-01-02"),
      (3, "Kitkat", 1000, "2021-01-03"),
      (4, "Kitkat", 2000, "2021-01-04"),
      (5, "Kitkat", 3000, "2021-01-05"),
      (6, "Kitkat", 1000, "2021-01-06")
    ).toDF("IT_Id","IT_Name","Price","PriceDate")

    kitkatPrice.show()
    // want to find the difference between the price on each day with it’s previous day.

    val window = Window.orderBy("Price")

    val df2 = kitkatPrice.withColumn("Prev_Price_Date",lag(col("Price"),1).over(window))
      .withColumn("PrifceDifference",col("Price")-col("Prev_Price_Date"))


    df2.show()


  }
}
