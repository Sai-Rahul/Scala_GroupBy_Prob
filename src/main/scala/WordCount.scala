import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{count, explode, split}


object WordCount {
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
    textData.show(false)

    val words = textData.select(explode(split($"Text","\\s")).alias("Word"))
    words.show()

    // Group by Word and calculate the count

    val WordCount = words.groupBy("Word")
      .agg(count($"Word").as("WordCount"))

    WordCount.show()



  }
}
