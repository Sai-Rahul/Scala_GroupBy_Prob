import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum

object RunningTotalRevenue_1 {

  def main(args:Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","RunningTotalRevenue_1")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

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
      ("Product6", "Category3", 180),
      ("Product1", "Category1", 200),
      ("Product2", "Category2", 400),
      ("Product3", "Category1", 350),
      ("Product4", "Category3", 600),
      ("Product5", "Category2", 150),
      ("Product6", "Category3", 500)
    ).toDF("Product", "Category", "Revenue")

    salesData.show()

    //1)Calculating the running total of revenue for each product.

    val windowSpec = Window.partitionBy("Product").orderBy("Category")

    val runningTotal = salesData.withColumn("Running_Cost",sum($"Revenue").over(windowSpec))

    runningTotal.show()
  }

}
