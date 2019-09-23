package qf.Class.sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

object ActionDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    //
    val rdd1 = sc.parallelize(List(1,2,3,4,5,10,9,8))
    //reduce fold aggregate
    val res0 = rdd1.reduce(_+_)
    //分区聚合，全局聚合都会使用初始化值进行结果的初始化
    val rdd2: RDD[Int] = rdd1.repartition(5)
    val res1 = rdd2.fold(100)(_+_)
    //rdd1.partitions.map((x:Partition)=>x.)
    val rdd3: RDD[String] = rdd1.mapPartitionsWithIndex((index:Int,it:Iterator[Int])=>it.map(index+":"+_))
    println(rdd3.collect().toBuffer)
    val res2 = rdd1.aggregate(100)(Math.max(_,_),_+_)
    //collect() collectAsMap():KEY VALUE
    val rdd4: RDD[(String, Int)] = rdd1.keyBy((x:Int)=>s"$x"+"aa")
    val res3: collection.Map[String, Int] = rdd4.collectAsMap()
    println(res3)
    //take first top takeordered
    val array: Array[(String, Int)] = rdd4.take(3)
    //top(num)=>takeOrdered(num)(ord.reverse)
    val array1: Array[(String, Int)] = rdd4.top(3)
    val array2: Array[(String, Int)] = rdd4.takeOrdered(3)
    //first=>take(1)
    val tuple: (String, Int) = rdd4.first()
    //参数1：是否放回，true：有放回，重复采样，参数2：采样个数：参数3：随机种子，有种子，伪随机，种子确定，每次采样的结果就是一定的
    val array3: Array[(String, Int)] = rdd4.takeSample(true,5,1000L)
    //transformation，按比例采样
    val rdd5: RDD[(String, Int)] = rdd4.sample(true,0.001,1000L)
    //返回元素个数
    val count: Long = rdd5.count()
      //(key,value)=>(key,count(value))
    val tupleToLong: collection.Map[(String, Int), Long] = rdd5.countByValue()
    //foreach
    //统计函数
    rdd4.max()
    rdd4.min()
    rdd1.mean()
    //返回一个近似值，不精确，但是很快，效率高
    rdd1.meanApprox(300)
    val res4: RDD[(Int, Long)] = rdd1.zipWithIndex()
    println(res4.collect().toBuffer)
    val rdd6: RDD[(Int, (String, Int))] = rdd1.zip(rdd4)
    println(array.toBuffer)
    println(array1.toBuffer)
    println(array2.toBuffer)
    println(array3.toBuffer)
    println(tuple)
//    println(rdd2.partitions.length)
    //    println(res2)
    sc.stop()
  }

}
