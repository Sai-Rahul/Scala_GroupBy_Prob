import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, min}

object CurrentMinimumSalary_6 {

  def main(args:Array[String]) : Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","CurrentMinimumSalary_6")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

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

    //Calculate the difference between the current salary and the minimum salary
    // within the last three rows, ordered by id

    val window = Window.orderBy("Id").rowsBetween(-2,0)

    val resultDF = SampleDF.withColumn("MinSalaryLast3Rows",
      min(col("Salary")).over(window))
      .withColumn("SalaryDifference",col("Salary")-col("MinSalaryLast3Rows"))


    resultDF.show()

  }


}
