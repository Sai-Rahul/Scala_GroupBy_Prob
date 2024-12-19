import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SalaryLead_Lag_2_VeryImportant {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "SalaryLead_Lag_2")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val Salary = List(
      (1, "John", 1000, "2021-01-01"),
      (2, "John", 2000, "2021-01-02"),
      (3, "John", 1000, "2021-01-03"),
      (4, "John", 2000, "2021-01-04"),
      (5, "John", 3000, "2021-01-05"),
      (6, "John", 1000, "2021-01-06")
    ).toDF("IT_Id", "IT_Name", "Price", "PriceDate")

    Salary.show()

    //2. If salary is less than previous month we will mark it as "DOWN", if salary has increased then "UP"

    val YearMonthCast = Salary.withColumn("YearMonth",date_format(to_date($"PriceDate"),"yyyy-MM"))
    YearMonthCast.show()

    val window = Window.orderBy("YearMonth")

    val dfwithLag = YearMonthCast.withColumn("Prev_Price",lag(col("Price"),1).over(window))

    dfwithLag.show()

    val result = dfwithLag.withColumn("Trend",
      when($"Price">$"Prev_Price","UP")
    .when($"Price"<$"Prev_Price","DOWN")
    .otherwise("No Change"))

    result.show()

  }


}
