package qf.Class.sparksqlday01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Person(name:String,age:Int,gender:String)
object dfDemo {

  def main(args: Array[String]): Unit = {
    //创建一个程序的sparkSession
    //spark 1.6 sqlContext hiveContext
    val sparkSession = SparkSession.builder().appName("df1").master("local[4]")
      .getOrCreate()
//    //方式2
//    val sparkConf= new SparkConf().setAppName("df1").setMaster("local[4]")
//    val sparkSession1 = SparkSession.builder().config(sparkConf).getOrCreate()
//    //支持hive
//    val sparkSession2 = SparkSession.builder().appName("df1").master("local[4]")
//        .enableHiveSupport()
//        .getOrCreate()
    //创建一个DataFrame
    //RDD=>DataFrame
    //创建RDD
   // val sc = new SparkContext()
    /*
    val sc = sparkSession.sparkContext
    val src: RDD[String] = sc.textFile("D://test.txt")
    val rdd = src.map(line=>{
      val arr: Array[String] = line.split(" ")
      val name = arr(0)
      val age = Integer.valueOf(arr(1))
      val gender = arr(2)
      Row(name,age,gender)
    })
    //定义这个数据集的结构信息scheme
   val strucType =  StructType(
      List(
        //第一个参数表示列名，列数据类型，是否可以为空
        StructField("name",StringType,true),
        StructField("age",IntegerType,true),
        StructField("gender",StringType,true)
      )
    )
    //RDD和结构信息关联
    val dataFrame: DataFrame = sparkSession.createDataFrame(rdd,strucType)
    //注册一个临时表或者视图
     dataFrame.registerTempTable("table_person")
    //兼容sql 2003标准
    val frame: DataFrame = sparkSession.sql("SELECT name,gender from table_person where age>20")
   //展示结果
    frame.show()
    sparkSession.stop()
    */
    //方式2 RDD=>dataFrame
    /*
    val sc = sparkSession.sparkContext
    val src: RDD[String] = sc.textFile("D://test.txt")
    val rdd:RDD[(String,Int,String)] = src.map(line=>{
      val arr: Array[String] = line.split(" ")
      val name = arr(0)
      val age = Integer.valueOf(arr(1))
      val gender = arr(2)
      (name,age,gender)
    })
    import sparkSession.implicits._
    val dataFrame: DataFrame = rdd.toDF("name","age","gender")
    dataFrame.registerTempTable("person_table")
    val frame: DataFrame = sparkSession.sql("SELECT * FROM person_table ORDER BY age desc")
    frame.show()
    sparkSession.stop()
    */
    //方式3 RDD=>dataFrame 反射形式
    val sc = sparkSession.sparkContext
    val src = sc.textFile("D://test.txt")
    val rdd:RDD[Person] = src.map(line=>{
      val arr = line.split(" ")
      Person(arr(0),arr(1).toInt,arr(2))
    })
    import sparkSession.implicits._
    val df: DataFrame = rdd.toDF
    //dsl风格编程
    val frame: DataFrame = df.select("name","age")
    frame.show()
    //dataFrame=>RDD
    frame.rdd

  }

}
