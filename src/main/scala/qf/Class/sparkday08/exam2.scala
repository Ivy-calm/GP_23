package qf.Class.sparkday08

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
case class CloseInfo(date:String,close:Double)
object exam2 {

  def main(args: Array[String]): Unit = {
    val sparkSession  = SparkSession.builder().appName("EXAM2").master("local[*]").getOrCreate()
    val lines: Dataset[String] = sparkSession.read.textFile("path")
    val filtered: Dataset[String] = lines.filter(line=>line.startsWith("2016"))
    import sparkSession.implicits._
    val mapped = filtered.map(line=>{
      val arr = line.split(",")
      val date = arr(0)
      val close = arr(4).toDouble
      CloseInfo(date,close)
    })
    import org.apache.spark.sql.functions._
    val frame: DataFrame = mapped.selectExpr("weekofyear(date) as week,close")
    val res: Dataset[Row] = frame.groupBy("week").agg(avg("close")).orderBy("week")
    res.show()
    sparkSession.stop()
  }

}
