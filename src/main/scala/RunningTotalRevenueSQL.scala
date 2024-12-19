import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RunningTotalRevenueSQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "RunningTotalRevenue_1")
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

    salesData.createOrReplaceTempView("salesData")

    //1)Calculating the running total of revenue for each product.

    val runningTotal = spark.sql(
      """SELECT
         Product,Category,Revenue,
         SUM(Revenue) OVER (PARTITION BY Category ORDER BY Product) AS RUNNING_Cost
         FROM salesData

        """)

    runningTotal.show()


  }
}
