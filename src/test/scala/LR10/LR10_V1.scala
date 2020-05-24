package LR10
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object LR10_V1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LR10_V1").setMaster("local[*]")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("Test app").getOrCreate()
    val datafile = spark.read
      .format("com.databricks.spark.csv")
      .option("header",true)
      .load("C:/Users/User/IdeaProjects/LR10/HRR_Scorecard.csv")
    //datafile.show()
        datafile.createOrReplaceTempView("HRR_Scorecard")
    // Максимальное количество прогнозируемых зараженных COVID-19 в США старше 18 лет
       spark.sql("SELECT MAX(`Projected Infected Individuals`) as max FROM HRR_Scorecard group by `HRR`").show()
    // Среднее количество пенсионеров в больницах США
       spark.sql("SELECT `HRR`, AVG(`Population 65+`) as avg FROM HRR_Scorecard group by `HRR`").show()
    // Количество мест в больницах Hospital и ICU
     spark.sql("select `Total ICU Beds`, `Total Hospital Beds`, `HRR` FROM HRR_Scorecard WHERE `Total Hospital Beds` > 100").show()

    spark.stop()
  }
}
