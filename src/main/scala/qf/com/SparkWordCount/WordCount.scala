package qf.com.SparkWordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val config = new SparkConf()
      .setAppName("Spark wordcount")
      //local[num]使用num个线程模拟集群执行任务
      //local[*]使用本地空闲线程模拟集群执行任务
      //local使用一个线程模拟集群执行任务
      .setMaster("local[2]")
    //spark程序的入口：sparkContext
    val sc = new SparkContext(config)
    //实现spark wordcount
    //加载数据集
    //sc.parallelize(List()),元素是文件里的每一行
    val source: RDD[String] = sc.textFile("c://tmp/1.txt",4)
    //RDD(word1,word2,......)
    val wordrdd: RDD[String] = source.flatMap(_.split(" "))
    //RDD((word1,1),(word2,1),......)
    val tuplerdd: RDD[(String, Int)] = wordrdd.map((_,1))
    //RDD((word1,10),(word2,4),......)
    val res0: RDD[(String, Int)] = tuplerdd.reduceByKey(_+_)
    //按单词个数排序，降序
    val sorted: RDD[(String, Int)] = res0.sortBy(_._2,false)
    //结果汇总到driver
    val res: Array[(String, Int)] = sorted.collect()
    println(res.toBuffer)
    //释放资源
    sc.stop()
  }

}
