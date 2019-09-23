package qf.Class.sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperationDemo2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("demo").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    val rdd1 = sc.parallelize(List(("english",90),("english",80),("english",70),("math",80),("english",75),
      ("math",86),("computer",90)))
    //计算学科总分
    rdd1.reduceByKey(_+_)
    rdd1.combineByKey((x:Int)=>x,(x:Int,y:Int)=>x+y,(x:Int,y:Int)=>x+y)
    //分区聚合，聚合的value（90,80,70）=》初始化结果acc = (90,1)=》(acc,80)=>acc = (90+80,1+1)=》（acc,70）=>acc=(170+70,2+1)
   // =》（240,3）
    //math:（80，86）=》80=》acc=（80,1）=》(acc,86)=>acc=(80+86,1+1)
    //english:(75)=>acc(75,1)
    //computer:(90)=>acc(90,1)
    //=>重新分区（哈希分区器=》相同key会在同一个分区，一个分区可能有多个key）
   // =>((240,3)(75,1))=>(240+75,3+1)=(330,4)
    //combineByKey
    //计算每个学科的平均成绩（学科，（学科总分，参与人数））
    //使用，(key,value)根据key分组，对组内的value进行聚合，当聚合的结果类型和value不一致的时候
    //使用combineByKey,第一个函数，定义类型转换
    val rdd2: RDD[String] = rdd1.mapPartitionsWithIndex((index:Int,it:Iterator[(String,Int)])=>it.map(index+":"+_))
    println(rdd2.collect().toBuffer)
    val rdd3 = rdd1.combineByKey((score:Int)=>
    {println("First fun:"+score);(score,1)},(acc:(Int,Int),score:Int)=>
      {  println("partition accumulate:"+acc._1+" "+acc._2+":"+score)
        (acc._1+score,acc._2+1)},
      (acc1:(Int,Int),acc2:(Int,Int))=>{
        println("merge all partition result:"+acc1._1+" "+acc1._2+":"+acc2._1+" "+acc2._2)
        (acc1._1+acc2._1,acc1._2+acc2._2)})
    //计算平均成绩
    val res = rdd3.collect().map((x:(String,(Int,Int)))=>(x._1,x._2._1/x._2._2))
    println(rdd3.collect().toBuffer)


  }

}
