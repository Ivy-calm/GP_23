package qf.Class.sparksqlday02

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

//自定义函数
// udf:map，一个输入对应一个输出,自定义一个函数
//udaf:聚合，多个输入对应一个输出：继承系统接口实现（DataFrame,DataSet）
//dataFrame类型，弱类型
//输入年龄，输出年龄的一个平均值
class myAggFun extends UserDefinedAggregateFunction{
  /*
  * A `StructType` represents data types of input arguments of this aggregate function.
    * For example, if a [[UserDefinedAggregateFunction]] expects two input arguments
    * with type of `DoubleType` and `LongType`, the returned `StructType` will look like
    *
    * */
  //定义聚合函数参数输入的数据类型
  def inputSchema: StructType={
    StructType{List(StructField("age",IntegerType,true))}
  }
  /**
    * A `StructType` represents data types of values in the aggregation buffer.
    * For example, if a [[UserDefinedAggregateFunction]]'s buffer has two values
    * (i.e. two intermediate values) with type of `DoubleType` and `LongType`,
    * the returned `StructType` will look like
    *
    * ```
    *   new StructType()
    *    .add("doubleInput", DoubleType)
    *    .add("longInput", LongType)
    * ```
    *
    * The name of a field of this `StructType` is only used to identify the corresponding
    * buffer value. Users can choose names to identify the input arguments.
    *
    * @since 1.5.0
    */
  //buffer的结构信息
  def bufferSchema: StructType={
    StructType{List(StructField("sum",IntegerType,true),StructField("count",IntegerType,true))}
  }

  /**
    * The `DataType` of the returned value of this [[UserDefinedAggregateFunction]].
    *
    * @since 1.5.0
    */
  //聚合函数的返回结果类型
  def dataType: DataType={
    DoubleType
  }

  /**
    * Returns true iff this function is deterministic, i.e. given the same input,
    * always return the same output.
    *
    * @since 1.5.0
    */
  //聚合函数，一个确定的输入，是否有一个确定的输出
  def deterministic: Boolean={true}

  /**
    * Initializes the given aggregation buffer, i.e. the zero value of the aggregation buffer.
    *
    * The contract should be that applying the merge function on two initial buffers should just
    * return the initial buffer itself, i.e.
    * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
    *
    * @since 1.5.0
    */
  //初始化每一个分区上进行聚合计算的容器
  def initialize(buffer: MutableAggregationBuffer): Unit={
    //初始化sum
    buffer(0) = 0
    //初始化分区上计算的count
    buffer(1) = 0
  }

  /**
    * Updates the given aggregation buffer `buffer` with new input data from `input`.
    *
    * This is called once per input row.
    *
    * @since 1.5.0
    */
  //遍历分区上每一个元素，然后把年龄累加到聚合结果上
  def update(buffer: MutableAggregationBuffer, input: Row): Unit={
    //更新分区上聚合的sum，count
    buffer(0)=buffer.getInt(0)+input.getInt(0)
    buffer(1)=buffer.getInt(1)+1
  }

  /**
    * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
    *
    * This is called when we merge two partially aggregated data together.
    *
    * @since 1.5.0
    */
  //多个分区上聚合结果进行汇总
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
    buffer1(0)=buffer1.getInt(0)+buffer2.getInt(0)
    buffer1(1)=buffer1.getInt(1)+buffer2.getInt(1)
  }

  /**
    * Calculates the final result of this [[UserDefinedAggregateFunction]] based on the given
    * aggregation buffer.
    *
    * @since 1.5.0
    */
  //返回聚合函数执行的最后结果
  def evaluate(buffer: Row): Double=buffer.getInt(0)/buffer.getInt(1).toDouble

}

//DataSet强类型
case class Person(name:String,age:Int,gender:String)
case class Avg(var sum:Int,var count:Int)
class myAgeavg extends Aggregator[Person,Avg,Double]{

  /**
    * A zero value for this aggregation. Should satisfy the property that any b + zero = b.
    * @since 1.6.0
    */
  //聚合结果的初始化
  def zero: Avg={
    Avg(0,0)
  }

  /**
    * Combine two values to produce a new value.  For performance, the function may modify `b` and
    * return it instead of constructing new object for b.
    * @since 1.6.0
    */
  //分区内聚合逻辑
  def reduce(b: Avg, a:Person): Avg={
    b.sum += a.age
    b.count += 1
    b
  }

  /**
    * Merge two intermediate values.
    * @since 1.6.0
    */
  def merge(b1: Avg, b2: Avg): Avg={
    b1.sum  += b2.sum
    b1.count += b2.count
    b1
  }

  /**
    * Transform the output of the reduction.
    * @since 1.6.0
    */
  def finish(reduction: Avg): Double={
    reduction.sum/reduction.count.toDouble
  }

  /**
    * Specifies the `Encoder` for the intermediate value type.
    * @since 2.0.0
    */
  def bufferEncoder: Encoder[Avg] = Encoders.product


  /**
    * Specifies the `Encoder` for the final ouput value type.
    * @since 2.0.0
    */
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object DefFunDemo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("udf").master("local[*]")
      .getOrCreate()

    val frame: DataFrame = sparkSession.read.parquet("person.parquet")
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
//    //自定义函数实现一个map功能
//    val fun = (x:Int)=>x+1
//    //注册我们自定义的函数
//    sparkSession.udf.register("changeAge",fun)
//    //使用自定义函数
//    import org.apache.spark.sql.functions._
//    val frame1: DataFrame = frame.selectExpr("age","changeAge(age)")
//    frame1.show()
    //使用自定义聚合函数
    //先注册函数
//
//    sparkSession.udf.register("ageAverage",new myAggFun )
//    val frame2: DataFrame = frame.agg(expr("ageAverage(age)"))
    //强类型自定义函数的使用

    val ds: Dataset[Person] = frame.as[Person]
    val averageAvg = new myAgeavg().toColumn.name("average_age")
    val result = ds.select(averageAvg)
    result.show()
    sparkSession.stop()
  }




}
