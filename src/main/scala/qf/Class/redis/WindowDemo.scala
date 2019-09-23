package qf.Class.redis

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object WindowDemo {
//rank denserank
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("window fun").master("local[2]")
      .getOrCreate()
   //rank
    val src = sparkSession.read.csv("D://data.csv").toDF("name","dep","salary")
    import org.apache.spark.sql.functions._
    val windowSpec: WindowSpec = Window.partitionBy(col("dep")).orderBy(col("salary") desc)
    //排名列
    //val column:Column = row_number.over(windowSpec)
    val column:Column = rank.over(windowSpec)
    val res: Dataset[Row] = src.select(col("name"),col("dep"),col("salary"),column.as("rank")).where(col("rank").leq(2))

    res.show()
    sparkSession.stop()
  }

}
