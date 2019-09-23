package qf.Class.sparksqlday02

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import sun.security.krb5.internal.PAData.SaltAndParams

object JdbcDemo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("jdbc").master("local[2]")
      .getOrCreate()
    //读数据库某个表中的文件
    val url="jdbc:mysql://ip:3306/db"
    val table="user"
    val properties = new Properties()
    properties.put("driver","com.mysql.jdbc.Driver")
    properties.setProperty("user","user")
    properties.setProperty("password","123456")
    val frame: DataFrame = sparkSession.read.jdbc(url,table,properties)
    val frame1: DataFrame = sparkSession.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://ip:3306/db","driver"->"com.mysql.jdbc.Driver",
      "user"->"user","password"->"123456")
    ).load()

    //dataframe写数据库
    frame.write.jdbc(url,table,properties)
    frame.write.option("url","jdbc:mysql://ip:3306/db")
      .option("driver","com.mysql.jdbc.Driver").save()

    frame.printSchema()
    sparkSession.stop

  }

}
