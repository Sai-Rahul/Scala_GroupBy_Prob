import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Purchase_6_SQL {
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
    val purchaseData = Seq(
      ("Customer1", "Product1", 100),
      ("Customer1", "Product2", 150),
      ("Customer1", "Product3", 200),
      ("Customer2", "Product2", 120),
      ("Customer2", "Product3", 180),
      ("Customer3", "Product1", 80),
      ("Customer3", "Product3", 250)
    ).toDF("Customer", "Product", "Amount")
    purchaseData.createOrReplaceTempView("purchaseData")

    // Group by Customer and calculate the count of distinct products

    val distinctProductCountByCustomer = spark.sql(
      """SELECT Customer,
         COUNT(DISTINCT Product) AS DISTINCT_PRODUCT
         FROM purchaseData
         GROUP BY Customer
        """)
    distinctProductCountByCustomer.show()

    // Group by Customer and calculate the sum of Amount

    val TotalAmount = spark.sql(
      """
        SELECT Customer,
        SUM(Amount) AS TOTAL_AMOUNT
        FROM purchaseData
        GROUP BY Customer
        """)

    TotalAmount.show()
  }
}
