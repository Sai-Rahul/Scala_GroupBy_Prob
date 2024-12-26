import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagNameChanges_16 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "LeadLagNameChanges_16")
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

    //Calculate the lead and lag of the salary column ordered by id,
    // but reset the lead and lag values when the employee’s name changes

    //Tip: Window.partitionBy("Name") ensures that the lead and lag operations are reset whenever the Name changes.
    // This means calculations are limited to rows within the same Name group.

    val window = Window.partitionBy("Name").orderBy("Id")

    val resultDF = SampleDF.withColumn("LeadSalary",lead(col("Salary"),1).over(window))
      .withColumn("LagSalary",lag(col("Salary"),1).over(window))

    resultDF.show()



  }
}

