import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Student_2_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "CustomerPurchase_SETB_9")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val scoreData = Seq(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")
    scoreData.createOrReplaceTempView("scoreData")

    //Finding the average score for each subject and the maximum score for each student.
    //Find the average score for each subject

    val AverageScorePerSubject = spark.sql(
      """SELECT subject,
         AVG(Score) AS Average_Score
         FROM scoreData
         GROUP BY subject

        """)

    AverageScorePerSubject.show()

    //Find the maximumscore for each subject

    val MaxScorePerSubject = spark.sql(
      """SELECT subject,
         MAX(Score) AS Max_Score
         FROM scoreData
         GROUP BY subject
        """)

    MaxScorePerSubject.show()


  }
}
