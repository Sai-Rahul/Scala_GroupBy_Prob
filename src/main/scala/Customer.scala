import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger,Level}
object Customer {

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

    // Group by Customer and calculate the count of orders
    val orderCountByCustomer = orderData.groupBy("Customer")
      .agg(count("OrderID").alias("OrderCount"))
    // Group by Customer and calculate the sum of Amount
    val totalAmountByCustomer = orderData.groupBy("Customer")
      .agg(sum("Amount").alias("TotalAmount"))
    orderCountByCustomer.show()
    totalAmountByCustomer.show()




  }
}
