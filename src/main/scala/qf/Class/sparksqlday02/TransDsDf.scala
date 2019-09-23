package qf.Class.sparksqlday02

import org.apache.spark.sql.{Dataset, SparkSession}
//DataFrame rdd
//DataFrame=>rdd .rdd
//rdd=>DataFrame toDF
case class people(name:String,age:Int)
object TransDsDf {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("jdbc").master("local[2]")
      .getOrCreate()
    //
    import sparkSession.implicits._
    val df = sparkSession.sparkContext.parallelize(Seq(("tom",20),("jerry",18),
      ("mary",22))).toDF("name","age")
    //dataFrame=>dataSet as[type]
    val ds: Dataset[people] = df.as[people]
    //dataSet=>dataFrame toDF()
    ds.toDF()
    //dataSet=>rdd  .rdd
    ds.rdd
    sparkSession.stop

  }
}
