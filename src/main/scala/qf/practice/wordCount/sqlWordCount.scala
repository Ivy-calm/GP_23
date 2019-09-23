package qf.practice.wordCount

import org.apache.spark
import org.apache.spark.sql.{DataFrame,Dataset,SparkSession}

object sqlWordCount {

  def main(args: Array[String]): Unit = {

    val  sparkSession = SparkSession.builder().appName("sqlWordCount").master("local[2]").getOrCreate()

    val data: Dataset[String] = sparkSession.read.textFile("C://tmp/1.txt")

    import sparkSession.implicits._
    val words: Dataset[String] = data.flatMap(_.split(" "))

    words.createTempView("v_wc")

    val result: DataFrame = sparkSession.sql("SELECT value, COUNT(*) counts FROM v_wc GROUP BY value ORDER BY counts DESC")


    result.show()
    sparkSession.stop()

  }

}
