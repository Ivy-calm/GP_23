//package qf.com.IpLocationSQL
//
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
//
//object IpLocationSQL {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().appName("JoinTest").master("local[2]").getOrCreate()
//
//    import spark.implicits._
//
//    val rluesDatasetLines:Dataset[String] = spark.read.textFile("C://tmp/ip.txt")
//
//    val rluesDataset = rluesDatasetLines.map(line => {
//
//      val fields = line.split("[|]")
//
//      val startNum = fields(2).toLong
//
//      val endNum = fields(3).toLong
//
//      val province = fields(6)
//      (startNum, endNum, province)
//
//    })
//    //收集ip规则到Driver端
//    val rulesInDriver: Array[(Long, Long, String)] = rluesDataset.collect()
//
//    //广播(必须使用sparkcontext)
//    // 将广播变量的引用返回到Driver端
//    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)
//
//    //创建RDD，读取访问日志
//    val accessLines: Dataset[String] = spark.read.textFile("C://tmp/order.log")
//
//    val ipDataFrame: DataFrame = accessLines.map(log => {
//
//      val fields = log.split("[|]")
//      val ip = fields(1)
////      val ipNum = MyUtils.ip2Long(ip)
//   //   ipNum
//    }).toDF("ip_num")
//
//
//    rluesDataset.createTempView("v_rules")
//    ipDataFrame.createTempView("v_ips")
//
//    val r = spark.sql("SELECT province, count(*) counts FROM v_ips JOIN v_rules ON (ip_num >= snum AND ip_num <= enum)" +
//      " GROUP BY province ORDER BY counts DESC")
//
//    r.show()
//
//    spark.stop()
//  }
//
//}
