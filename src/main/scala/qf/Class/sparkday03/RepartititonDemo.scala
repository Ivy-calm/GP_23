package qf.Class.sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RepartititonDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    val rdd1 = sc.parallelize(List(("english",90),("english",80),("english",70),("math",80),("english",75),
      ("math",86),("computer",90)))
    //重分区：调节分区个数，直接影响程序的并行度
    println(rdd1.partitions.length)
    //coalesce(numPartitions, shuffle = true)
    val rdd2: RDD[(String, Int)] = rdd1.repartition(10)
    println(rdd2.partitions.length)
    //分区数由多=》少，不发生shuffle
    val rdd3: RDD[(String, Int)] = rdd2.coalesce(5)
    println(rdd3.partitions.length)

  }

}
