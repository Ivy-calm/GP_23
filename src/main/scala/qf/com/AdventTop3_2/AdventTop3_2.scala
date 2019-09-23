package qf.com.AdventTop3_2

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
object AdventTop3_2 {

  def main(args: Array[String]): Unit = {
    //初始化环境
    val conf = new SparkConf().setAppName("adventtop3_2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //获取数据并切分
    val logsArr = sc.textFile("C://tmp/Advert.log").map(_.split("\t"))

    def getHour(time_long: Long) = {
      val datetime: DateTime = new DateTime(time_long)
      datetime.getHourOfDay.toString
    }

    //将数据整合成元组，便于聚合，key=省份+小时+广告id
    val proAndHourAndAdId: RDD[(String, Int)] = logsArr.map(arr => {
      val province = arr(1)
      val hour = getHour(arr(0).toLong)
      val adId = arr(4)
      (province + "_" + hour + "_" + adId, 1)
    })
    // 聚合生成每个省份的每个小时的每个广告的点击量
    val aggred: RDD[(String, Int)] = proAndHourAndAdId.reduceByKey(_+_)

      // 重新整合数据，便于接下来的分组排序
      val proAndHourTup: RDD[((String, String), String, Int)] = aggred.map(tup => {
        val splited = tup._1.split("_")
        val pro = splited(0)
      val hour = splited(1)
      val adId = splited(2)
      ((pro, hour), adId, tup._2)
    })

    // 用省份和小时进行分组
    val grouped: RDD[((String, String), Iterable[((String, String), String, Int)])] =
      proAndHourTup.groupBy(_._1)

    // 开始组内排序
    val res: RDD[((String, String), List[((String, String), String, Int)])] =
      grouped.mapValues(x => x.toList.sortWith(_._3 > _._3).take(3))

    println(res.collect.toBuffer)

    sc.stop()
  }
}
