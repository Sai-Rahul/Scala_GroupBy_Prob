import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object MaximumRevenue {
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

    //3. Finding the maximum revenue for each product category and the corresponding product.

    val maxRevenueByCategory = salesData.groupBy("Category")
      .agg(functions.max($"Revenue").alias("MaxRevenue"))

    maxRevenueByCategory.show()

    // Step 2: Join the original dataset with the max revenue dataset to find the product(s)
    val result = salesData
      .join(maxRevenueByCategory, salesData("Category") === maxRevenueByCategory("Category") && salesData("Revenue") === maxRevenueByCategory("MaxRevenue"))
      .select(salesData("Category"), salesData("Product"), salesData("Revenue"))

    result.show()




  }
}
