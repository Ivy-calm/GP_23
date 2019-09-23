package qf.Class.sparkday08

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object exam1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("exam1").setMaster("local[2]")
    val ssc= new StreamingContext(sparkConf,Seconds(10))
    val src: ReceiverInputDStream[String] = ssc.socketTextStream("node1",8989)
    //解析需要的字段
    val mappedDS = src.map(line=>
    {
      var arr = line.split(" ")
      val time = arr(0)
      val date = time.substring(0,7)
      val word = arr(2)
      (date+"_"+word,1)
    })
    val upstateFun = ( iterator: Iterator[(String, Seq[Int], Option[Int])])=>{
      iterator.map(line=>{
        (line._1,line._2.sum+line._3.getOrElse(0))
      })
    }
    //updateStateByKey
    //mapWithState
    //相同点：对实时数据进行全局的汇总，有状态的计算
    //区别：1）返回结果上，updateStateByKey返回的到当前所有数据的汇总结果（cogroup）
   //                     mapWithState返回的是当前批次出现的数据的汇总结果（map）
    // 2)效率上的区别:updateStateByKey效率低
    //                mapWithState效率高
    //3）超时删除机制：长时间（超时时间）范围内，没有出现数据，该数据的状态不再维护，从历史状态删除
    //过时的数据

    //4) mapWithState汇总的数据类型，和每一个批次RDD中数据类型（调用mapwithstate的RDD）可以不一致

    val accRes = mappedDS.updateStateByKey(upstateFun,new HashPartitioner(10),true)
  //mapWithState
    //reduceByKey,需要一个存储redis（key,value）
    //获取当前日期
    val date = new SimpleDateFormat("yyyyMMdd").format(new Date)
    //过滤拿到当天的数据，排序，取前10
    val res = accRes.filter((elem:(String,Int))=>elem._1 ==date).foreachRDD((rdd:RDD[(String,Int)])=>{
      rdd.sortBy(_._2,false).take(10)
    })

  }

}
