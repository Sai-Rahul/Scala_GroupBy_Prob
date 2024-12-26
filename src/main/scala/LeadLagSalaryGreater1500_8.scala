import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagSalaryGreater1500_8 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "LeadLagSalaryGreater1500_8")
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

//Calculate the lead and lag of the salary column for each employee ordered by id
    // but only for the employees who have a salary greater than 1500

    val window = Window.orderBy("Id")

    val LeadLagResultDF = SampleDF.withColumn("LeadSalary",lead(col("Salary"),1).over(window))
                                  .withColumn("LagSalary",lag(col("Salary"),1).over(window))

    LeadLagResultDF.show()

    val resultDF = LeadLagResultDF.filter($"Salary">1500)

    resultDF.show()
  }
}
