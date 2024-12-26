import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagSalaryIncreasing_14_VeryImportant {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "RunningTotalSalary_11")
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

    //Calculate the lead and lag of the salary column, ordered by id, but only for the employees whose salari
    //es are strictly increasing (i.e., each employee’s salary is greater than the previous employee’s salary)

    val window = Window.orderBy("Id")
 // Add a column to determine whether the salary is strictly increasing
    val SalaryIncreaseDF = SampleDF.withColumn("PreviousSalary",lag(col("Salary"),1).over(window))
      .filter(col("Salary")>col("PreviousSalary")|| col("PreviousSalary").isNull).drop("PreviousSalary")

    SalaryIncreaseDF.show()

    val resultDF = SalaryIncreaseDF.withColumn("LeadSalary",lead(col("Salary"),1).over(window))
                                   .withColumn("LagSalary",lag(col("Salary"),1).over(window))

    resultDF.show()



  }
}
