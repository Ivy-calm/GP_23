package qf.Class.sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KeyByDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)

    val rdd1 = sc.parallelize(List("scala","java","c"))
    //keyBy:给每一个元素按自定义逻辑生成该元素对应的key键（elem）=>(f(elem),elem)
    //((5,scala), (4,java), (1,c))
    val rdd2: RDD[(Int, String)] = rdd1.keyBy((x:String)=>x.length)
    //mapValues对元素类型是键值对的RDD进行操作，对值进行map
    //((5,scalaff), (4,javaff), (1,cff))
    val rdd3: RDD[(Int, String)] = rdd2.mapValues((value:String)=>value.concat("ff"))
    //((5,sc), (5,l), (5,ff), (4,j), (4,v), (4,ff), (1,cff))
    val rdd4: RDD[(Int, String)] = rdd3.flatMapValues((value:String)=>value.split("a"))
    println(rdd4.collect().toBuffer)
    sc.stop()
  }

}
