import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count}

object MovieRating_3_SQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "MovieRating_3_SQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val ratingsData = Seq(
      ("User1", "Movie1", 4.5),
      ("User2", "Movie1", 3.5),
      ("User3", "Movie2", 2.5),
      ("User4", "Movie2", 3.0),
      ("User1", "Movie3", 5.0),
      ("User2", "Movie3", 4.0)
    ).toDF("User", "Movie", "Rating")
    ratingsData.createOrReplaceTempView("ratingsData")

    // Group by Movie and calculate the average rating
    val AverageRating = spark.sql(
      """SELECT Movie,
         AVG(Rating) As Average_Rating
         FROM ratingsData
         GROUP BY Movie
        """)
    // Group by Movie and calculate the count of ratings

    val CountOfRating = spark.sql(
      """SELECT Movie,
         COUNT(Rating) As CountOf_Rating
         FROM ratingsData
         GROUP BY Movie
        """)
    AverageRating.show()
    CountOfRating.show()


  }
}
