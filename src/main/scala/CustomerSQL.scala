import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CustomerSQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "CustomerPurchase_SETB_9")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val orderData = Seq(
      ("Order1", "John", 100),
      ("Order2", "Alice", 200),
      ("Order3", "Bob", 150),
      ("Order4", "Alice", 300),
      ("Order5", "Bob", 250),
      ("Order6", "John", 400)
    ).toDF("OrderID", "Customer", "Amount")
    orderData.show()

    orderData.createOrReplaceTempView("orderData")

    // Group by Customer and calculate the count of orders

    val orderCountByCustomer = spark.sql(
      """ SELECT Customer,
          COUNT(OrderID) AS ORDER_COUNT
          FROM orderData
          GROUP BY Customer

        """)
    orderCountByCustomer.show()

    // Group by Customer and calculate the sum of Amount

    val TotalAmountByCustomer = spark.sql(
      """
        SELECT Customer,
        SUM(Amount) AS Total_Amount
        FROM orderData
        GROUP BY Customer
        """
    )

    TotalAmountByCustomer.show()

  }
}

