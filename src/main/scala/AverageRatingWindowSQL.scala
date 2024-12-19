import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AverageRatingWindowSQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "AverageRatingWindow_2")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val ratingData = Seq(
      ("User1", "Movie1", 4.5),
      ("User1", "Movie2", 3.5),
      ("User1", "Movie3", 2.5),
      ("User1", "Movie4", 4.0),
      ("User1", "Movie5", 3.0),
      ("User1", "Movie6", 4.5),
      ("User2", "Movie1", 3.0),
      ("User2", "Movie2", 4.0),
      ("User2", "Movie3", 4.5),
      ("User2", "Movie4", 3.5),
      ("User2", "Movie5", 4.0),
      ("User2", "Movie6", 3.5)
    ).toDF("User", "Movie", "Rating")
    ratingData.createOrReplaceTempView("ratingData")
    //2. Calculating the average rating for each user based on the last 3 ratings.

    val AverageRating = spark.sql(
      """
         SELECT
         User,Movie,Rating,
         AVG(Rating) OVER (
         PARTITION BY User
         ORDER BY Movie DESC
         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
         ) AS Avg_Rating
         FROM ratingData

        """)

    AverageRating.show()


  }
}
