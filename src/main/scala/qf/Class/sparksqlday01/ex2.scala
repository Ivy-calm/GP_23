package qf.Class.sparksqlday01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class log(phoneid:(String,String),time:Long)
object ex2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //需求：统计用户在哪个基站停留时间最长
    //log:18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
    //9F36407EAD8829FC166F14DDE7970F68,116.304864,40.050645,6
    //统计用户在每一个基站的停留总时长，排序，取top1
    //和基站的经纬度信息，join,用户的活动区域或者活动范围

    val src: RDD[String] = sc.textFile("d;\\")
    val rdd1 = src.map(line=>{
      val arr = line.split(" ")
      val phone = arr(0)
      var time = arr(1).toLong
      val id = arr(2)
      val state = Integer.valueOf(arr(3))
      if(state == 1)
        time = -time
      log((phone,id),time)
    })

    val rdd2: RDD[((String, String), Iterable[log])] = rdd1.groupBy(logobj=>logobj.phoneid)
    val rdd3: RDD[((String, String), List[((String, String), Long)])] = rdd2.mapValues(it=>it.toList.map(x=>((x.phoneid),x.time)))
    val rdd4: RDD[((String, String), ((String, String), Long))] = rdd3.mapValues(list=>list.reduce((x,y)=>(x._1,x._2+y._2)))
   //(每个用户在每个基站的停留时长)
    val rdd5: RDD[((String, String), Long)] = rdd4.map(x=>(x._1,x._2._2))

    //按用户的phone进行分组，组内top1


  }

}
