import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}

object LeadExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "MaximumRevenue")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)
    ).toDF("Product", "Category", "Revenue")

    salesData.show()

    val window = Window.orderBy("Revenue")

    val df2 = salesData.withColumn("Lead_Column",lead(col("Revenue"),2).over(window))

    df2.show()


  }
}
