package qf.Class.sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    //
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val rdd2 = rdd1.map((x:Int)=>{
      println(x)
      x*10
    })
    //什么时候缓存？
    //减少重复计算，实现基于RDD容错，提高程序的执行效率
    //1，rdd在多处复用，进行缓存，减少重复计算
    //2，计算逻辑复杂（比如机器学习算法）（计算依赖链特别长），将计算结果进行缓存
    //3，shuffle之后的RDD，进行缓存
    val cachedrdd: RDD[Int] = rdd2.cache()
//    val NONE = new StorageLevel(false, false, false, false)
//    val DISK_ONLY = new StorageLevel(true, false, false, false)
//    val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
//    val MEMORY_ONLY = new StorageLevel(false, true, false, true)
//    val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
//    val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
//    val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
//    val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
//    val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
//    val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
//    val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
//    val OFF_HEAP = new StorageLevel(true, true, true, false, 1)
    cachedrdd.filter(_%2==0)
    val persistrdd: RDD[Int] = rdd2.persist()
    rdd2.persist(StorageLevel.MEMORY_ONLY)

    println(cachedrdd.collect().toBuffer)
    println(cachedrdd.collect().toBuffer)

  }

}
