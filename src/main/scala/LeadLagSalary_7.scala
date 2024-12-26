import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagSalary_7 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "CurrentMinimumSalary_6")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()
    import spark.implicits._


    val SampleDF = Seq(
      (1, "Alice", 1000),
      (2, "Alice", 2000),
      (3, "Bob", 1500),
      (4, "Bob", 3000),
      (5, "Charlie", 2500),
      (6, "Charlie", 3500)
    ).toDF("Id", "Name", "Salary")

    SampleDF.show()

    //7.Calculate the lead and lag of salary
    // within each group of employees (grouped by name) ordered by id.

    val window = Window.partitionBy("Name").orderBy("Id")

    val resultDF = SampleDF.
       withColumn("LeadSalary",lead(col("Salary"),1).over(window))
      .withColumn("LagSalary",lag(col("Salary"),1).over(window))
    resultDF.show()



  }
}
