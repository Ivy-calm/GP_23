package qf.Class.sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Exercise1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    //统计学科模块的top3=>分组top3
//    20161123101523	http://java.learn.com/java/javaee.shtml
//    20161123101523	http://java.learn.com/java/javaee.shtml
//    20161123101523	http://ui.learn.com/ui/video.shtml
//    20161123101523	http://bigdata.learn.com/bigdata/teacher.shtml
  //(学科，模块，访问次数)
    val src: RDD[String] = sc.textFile("dir/access.txt")
    //RDD((ui.learn.com/ui/video.shtml,1))
    val rdd1=src.map((line:String)=>{
      val arr: Array[String] = line.split("//")
      (arr(1),1)
    })
    //RDD(("ui.learn.com/ui/video.shtml",count))
    val reduced: RDD[(String, Int)] = rdd1.reduceByKey(_+_)
   val rdd2:RDD[(String,(String,Int))] =  reduced.map((x:(String,Int))=>{
      val arr: Array[String] = x._1.split("/")
      //(学科，（模块，模块的访问次数）)
      (arr(0),(x._1,x._2))
    })
    //先分组，组内排序，组内取top3
    //(学科，it（模块，模块的访问次数）)
    val grouped: RDD[(String, Iterable[(String, Int)])] = rdd2.groupByKey()
    val res: RDD[(String, List[(String, Int)])] = grouped.mapValues((x:Iterable[(String,Int)])=>x.toList.sortBy(_._2).reverse.take(3))
    //先全局按访问次数排序，再分组，取top3
    val sorted:RDD[(String,(String,Int))] = rdd2.sortBy(((x:(String,(String,Int)))=>x._2._2),false)
    sorted.groupByKey().mapValues(x=>x.take(3))


    val array: Array[(String, List[(String, Int)])] = res.collect()

    println(res.collect.toBuffer)
    sc.stop()


  }

}
