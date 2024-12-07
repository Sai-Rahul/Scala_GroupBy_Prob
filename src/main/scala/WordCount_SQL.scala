import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "WordCount")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val textData = Seq(
      "Hello, how are you?",
      "I am fine, thank you!",
      "How about you?"
    ).toDF("Text")
    textData.createOrReplaceTempView("textData")


    val words = spark.sql(
      """
        SELECT
        explode(split(Text , '\\s+')) as word
        FROM textData
        """)

    words.createOrReplaceTempView("words")


    // Group by Word and calculate the count

    val wordOccurence = spark.sql(
      """SELECT word,
         COUNT(word) AS Total_Count
         FROM words
         GROUP BY Word

        """)

    wordOccurence.show()

  }
}