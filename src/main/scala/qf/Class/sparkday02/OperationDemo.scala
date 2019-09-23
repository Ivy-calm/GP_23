package qf.Class.sparkday02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperationDemo {

  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val sparkConf = new SparkConf().setAppName("operation damo")
      .setMaster("local[*]")
    val sc= new SparkContext(sparkConf)
    //创建一个RDD
    val rdd1: RDD[Int] = sc.parallelize(0 to 100,4)
    val rdd2: RDD[String] = sc.textFile("D://test.txt",10)
    //map
    val mapRDD: RDD[Int] = rdd1.map(_*10)
    //filter
    val filteredRDD: RDD[Int] = rdd1.filter(_%2==0)
    //    val array: Array[Int] = mapRDD.collect()
    //    println(array.toBuffer)
    //mapPartition
    //map和mapPartition的区别 mapPartitions更高效，有可能会导致OOM，内存充足的情况下，
    // 使用场景，比如数据库写入，一次连接，写入一个分区的数据，使用带有Partitions结尾的算子。
    val mapPartionsRDD: RDD[Int] = rdd1.mapPartitions((it:Iterator[Int])=>it.map(_*10))
    //mapPartitionsWithIndex和mapPartionsRDD相同点：基于分区数据进行计算，mapPartitionsWithIndex可以
    // 获取到数据对应的分区号
    val mapPartitionsWithIndexRDD: RDD[String] = rdd1.mapPartitionsWithIndex((index:Int,it:Iterator[Int])=>it.map(index+":"+_))
    println(mapPartitionsWithIndexRDD.collect().toBuffer)
    //flatmap=>map,map的结果是集合+flattern
    //map后rdd(range(0),range(0,1),range(0,1,2))
    //flattern后rdd（0,0,1,0，1,2，。。。。）
    val flatMapRDD: RDD[Int] = rdd1.flatMap((x:Int)=> 0 to x)
    //  println(flatMapRDD.collect().toBuffer)
    //foreach
    val unit: Unit = flatMapRDD.foreach(println)
    //    println(mapPartionsRDD.collect().toBuffer)
    // println(filteredRDD.collect().toBuffer)
    //释放资源
    //
    val rdd3 = sc.parallelize(List("scala","scala","scala","scala","java","scala","c++","python","c++"))
    //(key,compactBuffer(elem,elem))
    //((python,CompactBuffer(python)), (scala,CompactBuffer(scala, scala)), (java,CompactBuffer(java))
    val grouped: RDD[(String, Iterable[String])] = rdd3.groupBy((x:String)=>x)
    val rdd4: RDD[(String, Int)] = rdd3.map((_,1))
    //rdd((key,it(value1,value2......)))
    //RDD((python,CompactBuffer(1)), (scala,CompactBuffer(1, 1)), (java,CompactBuffer(1)), (c++,CompactBuffer(1, 1)))
    val grouped2: RDD[(String, Iterable[Int])] = rdd4.groupByKey()
    // println(grouped2.collect().toBuffer)
    val res = grouped2.mapValues(_.size)
    //RDD((python,1), (scala,2), (java,1), (c++,2))
    //groupByKey分组 reduceByKey分组之后，value进行聚合，局部聚合（每个分区的数据聚合）+全部聚合（每个分区的聚合结果再进行
    // 汇总聚合，分区聚合和全局聚合的逻辑（传入的定义函数）是一样的）
    val reduced: RDD[(String, Int)] = rdd4.reduceByKey(_+_)

    val mappedWithIndex = rdd4.mapPartitionsWithIndex((index:Int,it:Iterator[(String,Int)])=>it.map(index+":"+_))
    println(mappedWithIndex.collect().toBuffer)
    //foldByKey先进行分区聚合，再全局汇总，初始化值在分区局部聚合的时候使用
    val folded: RDD[(String, Int)] = rdd4.foldByKey(100)(_+_)

    val aggregated: RDD[(String, Int)] = rdd4.aggregateByKey(100)((x:Int,y:Int)=>Math.min(x,y),(x:Int,y:Int)=>x+y)
    //    println(reduced.collect().toBuffer)
    //    println(folded.collect().toBuffer)
    println(aggregated.collect().toBuffer)
    //排序
    //按(key,value)中的value进行排序,降序
    aggregated.sortBy((x:(String,Int))=>x._2,false)
    //按(key,value)中的key进行排序,降序
    aggregated.sortByKey(false)
    sc.stop()
  }

}
