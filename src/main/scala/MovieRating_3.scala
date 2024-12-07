import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count}
import org.apache.log4j.{Logger,Level}
//Finding the average rating for each movie and the total number of ratings for each movie.
object MovieRating_3 {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "MovieRating_3")
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
    ratingsData.show()

    // Group by Movie and calculate the average rating
     val AverageRating = ratingsData.groupBy("movie")
       .agg(avg($"Rating").as("Average_Rating"))
    // Group by Movie and calculate the count of ratings

    val CountOfRating = ratingsData.groupBy("movie")
      .agg(count($"Rating").as("CountOf_Rating"))

    AverageRating.show()
    CountOfRating.show()

  }
}
