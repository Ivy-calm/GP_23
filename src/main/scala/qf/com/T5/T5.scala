package qf.com.T5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object T5 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("operation damo")
      .setMaster("local[*]")
    val sc= new SparkContext(sparkConf)

    //创建RDD
    val rdd1: RDD[Int] = sc.parallelize(0 to 100,4)   //得到一个数据集

   // val rdd2: RDD[String] = sc.textFile("dir/access.txt") //从本地文件中得到一个字符串类型的RDD

    val maprdd: RDD[Int] = rdd1.map(_*10)

    val array: Array[Int] = maprdd.collect()

    val filteredrdd: RDD[Int] = rdd1.filter(_%2==0)
    
    println(array.toBuffer)

    println(filteredrdd.collect().toBuffer)

    sc.stop()
  }

}
