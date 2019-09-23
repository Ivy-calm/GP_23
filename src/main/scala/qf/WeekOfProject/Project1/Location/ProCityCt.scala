package qf.WeekOfProject.Project1.Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object ProCityCt {

  def main(args: Array[String]): Unit = {

    if(args.length != 1){

      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath) = args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
    val df =  spark.read.parquet(inputPath)
    df.createTempView("log")

    val df2 = spark.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //存Mysql

    val load = ConfigFactory.load()

    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))


    df2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),prop)

    spark.stop()

  }

}
