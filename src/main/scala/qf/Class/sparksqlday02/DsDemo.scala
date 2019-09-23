package qf.Class.sparksqlday02

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
case class Person0(name:String,age:Int,gender:String)
object DsDemo {
  //读写的数据源
  //text,csv,json,parquet,jdbc
  //parquet,sparksql默认的文件格式
  //parquet按列存储，压缩率高
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val sparkSession = SparkSession.builder()
      .appName("ds demo").master("local[2]")
      .getOrCreate()
    val frame: DataFrame = sparkSession.read.text("D://test.txt")
    import sparkSession.implicits._
    val frame1 = frame.map(row=>{
      val str = row.getString(0)
      val arr =  str.split(" ")
      Person(arr(0),arr(1).toInt,arr(2))
    })

//    frame1.write.format("csv").save("person.csv")
//    frame1.write.csv("person.csv")
    //存储模式：error（default）,append,overwrite,ignore
//    frame1.write.mode("append").save("person.csv")
//    frame1.write.parquet("person.parquet")
//    frame1.write.json("person.json")
    //只能存储一列
   // frame1.write.text("person.txt")
    //打印schema信息
    //读文件
//    sparkSession.read.format("csv").load("person.csv")
  //val frame2 =  sparkSession.read.csv("person.csv")
//    sparkSession.read.json("person.json")
    val frame2: DataFrame = sparkSession.read.parquet("person.parquet")
    frame2.printSchema()
    frame2.show()
    sparkSession.stop()
  }

}
