import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object HighestSales_7_Very_Important {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Weather_5")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val salesData = Seq(
      ("Product1", 100),
      ("Product2", 200),
      ("Product3", 150),
      ("Product4", 300),
      ("Product5", 250),
      ("Product6", 180)
    ).toDF("Product", "SalesAmount")

    salesData.show()

    //Finding the top N products with the highest sale Amount

    val N =3;

    val TopProducts = salesData.orderBy($"SalesAmount".desc)
      .limit(3)

    TopProducts.show()





  }
}
