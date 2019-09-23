package qf.Class.sparksqlday01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object ex1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //数据格式: y province city userid adid
    val source: RDD[String] = sc.textFile("d://")
    //按省份，统计广告点击次数前三
    val rdd = source.map(x => {
      val arr: Array[String] = x.split(" ")
      val province = arr(1)
      val adid = arr(4)
      (province, adid)
    })
//    //(省份，（adid,count）)
//    val rdd1: RDD[(String, (String, Int))] = rdd.map(tup => (tup._1, (tup._2, 1)))
//    //(省份，it（(adid,1),(adid,1)）)
//    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd1.groupByKey()
//    rdd2.mapValues((it: Iterable[(String, Int)]) => {
//      val r1: Map[String, Iterable[(String, Int)]] = it.groupBy(_._1)
//      //（省份，（adid,聚合count））
//      val tuples: List[(String, (String, Int))] = r1.toList.map(x => (x._1, x._2.reduce((acc: (String, Int), x: (String, Int)) => {
//        (acc._1, acc._2 + x._2)
//      })))
//      val res: List[(String, (String, Int))] = tuples.sortBy(x => x._2._2).reverse.take(3)
//
//    })
//
    val rdd1: RDD[((String, String), Int)] = rdd.map(x=>((x._1,x._2),1))
    //((省份，adid),total)
    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_+_)
    //((省份，adid),total)=>(省份，(adid,total))
    val rdd3: RDD[(String, (String, Int))] = rdd2.map(x=>(x._1._1,(x._1._2,x._2)))
    //(省份，(adid,total)=>(省份，it((省份，(adid,total),(省份，(adid,total)......))
    val rdd4: RDD[(String, Iterable[(String, (String, Int))])] = rdd3.groupBy(x=>x._1)
    val rdd5: RDD[(String, List[(String, (String, Int))])] = rdd4.mapValues((it:Iterable[(String,(String,Int))])=>it.toList.sortBy(_._2._2).reverse.take(3))
    val res: Array[(String, List[(String, (String, Int))])] = rdd5.collect()

    //统计每一个省份每一个小时的TOP3广告ID

    //
    def getHour(timeStamp:String):String={
      val time = new DateTime(timeStamp)
      val hour: String = time.getHourOfDay.toString
      hour
    }
    val rdd01 = source.map(x => {
      val arr: Array[String] = x.split(" ")
      val province = arr(1)
      val adid = arr(4)
      val hour = getHour(arr(0))
      ((province,hour, adid),1)
    })
    val rdd02: RDD[((String, String, String), Int)] = rdd01.reduceByKey(_+_)
    val rdd03: RDD[((String, String), (String, Int))] = rdd02.map(x=>((x._1._1,x._1._2),(x._1._3,x._2)))
   //((省份，小时)，it（(adid,count),(adid,count).....）)
    val rdd04: RDD[((String, String), Iterable[(String, Int)])] = rdd03.groupByKey()
    val rdd05: RDD[((String, String), List[(String, Int)])] = rdd04.mapValues(it=>it.toList.sortBy(_._2).reverse.take(3))
    rdd05.collect()


  }

}
