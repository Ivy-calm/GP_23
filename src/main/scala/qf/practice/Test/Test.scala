package Test

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 访问时间 用户id 查询词 该URL在返回结果中的排名 用户点击的顺序号 用户点击的URL
  */
object Test {
  def main(args: Array[String]): Unit = {
    var num = 1
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "node01:9092",
      // 指定key的反序列化方式
      "key.deserializer" -> classOf[StringDeserializer],
      // 指定value的反序列化方式
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // 指定消费位置
      "auto.offset.reset" -> "latest",
      // 如果value合法，自动提交offset
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    // 指定topic
    val topics = Array("test")
    // 消费数据
    val massage: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaParam)
      )
    massage.map(x => {
      val logSplited = x.value().split("\t")
      val time = formatDate(logSplited(0).toLong)
      val words = logSplited(2)
      ((time, words), 1)
    }).updateStateByKey(func)
      .transform(rdd => rdd.sortBy(_._2, false))
      .foreachRDD(rdd => rdd.foreachPartition(it => {
        while (it.hasNext) {
          val tuple: ((String, String), Int) = it.next()
          val time = tuple._1._1
          val word = tuple._1._2
          if(num == 10) return
          else {
            println("时间" + time + "热度第" + num + ":" + word)
            num += 1
          }
        }
      }))



    ssc.start()
    ssc.awaitTermination()
  }
  def func = (values:Seq[Int],state:Option[Int]) => {
    var clickCount = 0
    if(state.nonEmpty) clickCount = state.getOrElse(0)
    for (value <- values) clickCount += value
    Option(clickCount)

  }
  /**
    * 通过时间戳来获取yyyyMMdd格式的日期
    */
  def formatDate(timestamp: Long) = new SimpleDateFormat("yyyyMMdd").format(new Date(timestamp))

}
