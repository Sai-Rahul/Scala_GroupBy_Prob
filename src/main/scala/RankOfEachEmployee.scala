import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, rank}

object RankOfEachEmployee {

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

    //Calculate the rank of each employee based on their salary,
    // ordered by salary in descending order.

    val window = Window.orderBy(col("Salary").desc)

    val resultDF = SampleDF.withColumn("Rank",dense_rank().over(window))

    val resultDF1 = SampleDF.withColumn("Rank",rank().over(window))


    resultDF1.show()

    resultDF.show()




  }
}
