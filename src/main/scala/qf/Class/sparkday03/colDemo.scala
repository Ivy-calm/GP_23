package qf.Class.sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object colDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    //
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val rdd2 = sc.parallelize(List(1,2,3,4,4,5,7,8,9,10))
    //union,要求两个rdd，其中元素类型一致,没有去重
    val res1 = rdd1.union(rdd2)
   // println(res1.collect().toBuffer)
    //对rdd中元素去重
    val res2: RDD[Int] = res1.distinct()
  //  println(res2.collect().toBuffer)
    //intersection返回两个RDD中的公共元素,要求两个RDD中元素类型一致
    val res3: RDD[Int] = rdd1.intersection(rdd2)
   // println(res3.collect().toBuffer)
    //join要求rdd中的元素是key value键值对
    val rdd3 = sc.parallelize(Array(("tom",18),("tom",19),("jerry",18),("candy",25),("mary",30)))
    val rdd4 = sc.parallelize(Array(("tom",20),("mike",18),("kate",25),("mary",30)))
    val rdd5 = sc.parallelize(Array(("tom",28),("mike",28),("kate",28),("mary",28)))
    //res:((tom,(20,20)), (mary,(30,30)))
    val res4: RDD[(String, (Int, Int))] = rdd3.join(rdd4)
    //左外连接，右外连接，逻辑同MySQL表连接
    //((tom,(20,Some(20))), (jerry,(18,None)), (candy,(25,None)), (mary,(30,Some(30))))
    val res5: RDD[(String, (Int, Option[Int]))] = rdd3.leftOuterJoin(rdd4)
   // ((tom,(Some(20),Some(20))), (mike,(None,Some(18))), (jerry,(Some(18),None)).......
    val res6: RDD[(String, (Option[Int], Option[Int]))] = rdd3.fullOuterJoin(rdd4)
    //笛卡尔积
    //(((tom,20),(tom,20)), ((tom,20),(mike,18)), ((jerry,18),(tom,20))......
    val res7: RDD[((String, Int), (String, Int))] = rdd3.cartesian(rdd4)
    //cogroup可以对两个以上RDD进行连接，结果集中元素，(key,value),所有RDD中的key都会在结果集中出现
    //value是每一个RDD中每一个key对应的value组成的可迭代集合的元组
    //((tom,(CompactBuffer(18, 19),CompactBuffer(20),CompactBuffer(28))),。。。。。。
    val res8: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rdd3.cogroup(rdd4,rdd5)
    //println(res7.count())
    println(res8.collect().toBuffer)
    sc.stop()

  }

}
