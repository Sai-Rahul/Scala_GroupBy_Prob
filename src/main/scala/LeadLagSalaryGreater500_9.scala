import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagSalaryGreater500_9 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "LeadLagSalaryGreater500_9")
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

    //Calculate the lead and lag of the salary column for each employee,
    // ordered by id, but only for the employes
    // who have a change in salary greater than 500 from the previous row

    val window = Window.orderBy("Id")

    //lag of salary to get the previous salary

    val lagDF = SampleDF.withColumn("PreviousSalary", lag(col("Salary"),1).over(window))

    lagDF.show()

    //salary greater than 500 from the previous row

    val filteredDF = lagDF.withColumn("SalaryDifference", col("Salary")-col("PreviousSalary"))
      .filter(col("SalaryDifference")>500)

    filteredDF.show()

    val resultDF = filteredDF.withColumn("LeadSalary",lead(col("Salary"),1).over(window))
                             .withColumn("LagSalary",lag(col("Salary"),1).over(window))

    resultDF.show()




  }
}
