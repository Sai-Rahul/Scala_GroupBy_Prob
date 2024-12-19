import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}

object SalaryPercentageLeadLag4 {

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

    val SampleDF = Seq(
      (1, "Alice", 1000),
      (2, "Bob", 2000),
      (3, "Charlie", 1500),
      (4, "David", 3000)
    ).toDF("Id", "Name", "Salary")

    SampleDF.show()
    //Calculate the percentage change in salary from the previous row to the current row, ordered by id.
    //(using the same sample data)

    val window = Window.orderBy("Id")

    val SalaryLag = SampleDF.withColumn("Prev_Salary",lag(col("Salary"),1).over(window))

    SalaryLag.show()

    val PercentageDiff =SalaryLag.withColumn("PercentageChange",
      when(col("Prev_Salary").isNull,"Null")
    .otherwise(((col("Salary")-col("Prev_Salary"))/col("Prev_Salary"))*100)
    )

    PercentageDiff.show()


  }
}
