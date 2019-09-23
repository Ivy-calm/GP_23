package qf.practice.LDsparkSqlTest

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("Test").master("local[2]").getOrCreate()

    val frame: DataFrame = sparkSession.read.json("dir/JsonTest02.json")


    frame.createTempView("jsonTest")

    sparkSession.sql("select phoneNum user,sum(money) sum from jsonTest where status='1' group by phoneNum order by sum desc").show()

    sparkSession.sql("select terminal,count(phoneNum) count from jsonTest group by terminal order by count desc").show()

    sparkSession.sql(" select tmp1.* from (select tmp.phoneNum as phone,tmp.province as pro,tmp.num as sum,row_number() over(distribute by tmp.province sort by tmp.num desc) as ranks from (select phoneNum,province, count(1) as num from jsonTest group by phoneNum,province) tmp )tmp1 where tmp1.ranks < 4 ").show()


  }

}
