import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}

object RollingSum_LeadLag_5 {

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

    //5.Calculate the rolling sum of salary for the current row and the previous two rows, ordered by id.

    val window = Window.orderBy("Id").rowsBetween(-2, 0)

    val RollingSumDF = SampleDF.withColumn("RollingSumSalary", sum(col("Salary")).over(window))

    RollingSumDF.show()
  }
}
