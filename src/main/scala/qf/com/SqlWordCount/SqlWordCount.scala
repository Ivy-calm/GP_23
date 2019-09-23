package qf.com.SqlWordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SqlWordCount {

  def main(args: Array[String]): Unit = {
    //创建一个入口点
    val sparkSession = SparkSession.builder().appName("sql word count").master("local[2]")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd: RDD[String] = sc.textFile("C://tmp/test.txt")
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    import sparkSession.implicits._
    val frame:DataFrame = word.toDF("word")
    //sql 风格
//    frame.registerTempTable("table_word")
//    val dataFrame: DataFrame = sparkSession.sql("select word,count(*) as count from table_word group by word order by count desc")
//    dataFrame.show()
    //dsl风格
    //val value: Dataset[Row] = frame.as("word")
    val line: Dataset[String] = sparkSession.read.textFile("C://tmp/test.txt")
    val wordDs: Dataset[String] = line.flatMap(_.split(" "))
    //默认的列名：value
    import org.apache.spark.sql.functions._
    val sorted: Dataset[Row] = wordDs.groupBy($"value" as "word").agg(count("*") as "count").orderBy($"count" desc)
    sorted.show()
    sparkSession.stop()
  }

}
