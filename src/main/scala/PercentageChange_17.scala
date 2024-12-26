import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object PercentageChange_17 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "PercentageChange_17")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val SampleDF = Seq(
      (1, "Alice", 1000),
      (2, "Bob", 2000),
      (3, "Alice", 1500),
      (4, "David", 3000),
      (5, "Alice", 1800),
      (6, "David", 3200)
    ).toDF("Id", "Name", "Salary")

    SampleDF.show()

    //Calculate the percentage change in salary from the previous row to the current row,
    // ordered by id, but group the percentage changes by name.

    val window = Window.partitionBy("Name").orderBy("Id")

    val PreviousSalaryDF = SampleDF.withColumn("PreviousSalary", lag(col("Salary"), 1).over(window))
    PreviousSalaryDF.show()
    val percentageChangeDF = PreviousSalaryDF.withColumn("PercentageSalary", (col("Salary")-col("PreviousSalary"))/col("PreviousSalary")*100).drop("PreviousSalary")

    percentageChangeDF.show()

  }
}
