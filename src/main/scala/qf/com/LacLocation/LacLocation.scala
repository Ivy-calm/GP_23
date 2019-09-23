package qf.com.LacLocation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LacLocation {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf对象进行配置,然后在创建SparkContext进行操作
    val conf = new SparkConf().setAppName("BaseStationDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //2.获取用户访问基站的log日志
    //ps:因为使用是绝对路径所以,你在使用时候需要修改这个路径
    val files: RDD[String] = sc.textFile("dir/log")

    //3.切分用户log日志
    val userInfo: RDD[((String, String), Long)] = files.map(line => {
      val fields: Array[String] = line.split(",") //切分
      val phone = fields(0) //用户手机号
      val time = fields(1).toLong //时间戳(数字)
      val lac = fields(2) //基站ID
      val eventType = fields(3)
      //时间类型(连接后断开)
      //连接时长需要进行一个区分,因为进入基站范围内有两种状态,这个状态决定时间的开始于结束
      val time_long = if (eventType.equals("1")) -time else time
      //元组 手机号和基站作为key  时间作为value
      ((phone, lac), time_long)
    })
    //用户在相同基站所停留总时长
    val sumed: RDD[((String, String), Long)] = userInfo.reduceByKey(_+_)

    //为了便于和基站信息进行join此时需要将数据进行一次调整
    //基站ID作为key  手机号和时长作为value
    val lacAndPT: RDD[(String, (String, Long))] = sumed.map(tup => {
      val phone = tup._1._1
      //用户手机号
      val lac = tup._1._2 //基站ID
      val time = tup._2 //用户在某个基站所停留的总时长
      (lac, (phone, time))
    })

    //获取基站的基础数据
    val lacInfo = sc.textFile("dir/lac_info.txt")

    //切分基站的书数据
    val lacAndXY: RDD[(String, (String, String))] = lacInfo.map(line => {
      val fields: Array[String] = line.split(",")
      val lac = fields(0) //基站ID
      val x = fields(1) // 经度
      val y = fields(2) //纬度
      (lac, (x, y))
    })

    //把经纬度的信息join到用户信息中
    val joined: RDD[(String, ((String, Long), (String, String)))] = lacAndPT join lacAndXY

    //为了方便以后的分组排序,需要进行数据整合
    val phoneAndTXY: RDD[(String, Long, (String, String))] = joined.map(tup => {
      val phone = tup._2._1._1 //手机号
      val time = tup._2._1._2 //时长
      val xy: (String, String) = tup._2._2 //经纬度
      (phone, time, xy)
    })

    //按照用户的手机号进行分组
    val grouped: RDD[(String, Iterable[(String, Long, (String, String))])] = phoneAndTXY.groupBy(_._1)
    //按照时长进行组内排序
    val sorted: RDD[(String, List[(String, Long, (String, String))])] = grouped.mapValues(_.toList.sortBy(_._2).reverse)

    //数据进行整合
    val res: RDD[(String, List[(Long, (String, String))])] = sorted.map(tup => {
      val phone = tup._1
      //手机号
      val list = tup._2 //存储数据的集合55
      val filterList = list.map(tup1 => { //这个集合中的手机号顾虑掉
        val time = tup1._2 //时长
      val xy = tup1._3
        (time, xy)
      })
      (phone, filterList)
    })
    //取值
    val ress = res.mapValues(_.take(2))
    println(ress.collect.toList)
    sc.stop()
  }
}
