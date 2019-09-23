package qf.Class.sparkday01

import scala.io.{BufferedSource, Source}

object Exam {

  def main(args: Array[String]): Unit = {
    val list1 = (1 to 100).toList
    list1.filter(_%2==0)
    list1.map((x:Int)=>x%2==0)

    def match1(x:Int)={
      x match {
        case 1=>println("")
        case _=>println("")
      }

    }
    //9
    def exam9_1(path:String)={
      val bufferedSource: BufferedSource = Source.fromFile(path)
      val lines: Iterator[String] = bufferedSource.getLines()
     // ("line ",0)
      val index: List[(String, Int)] = lines.toList.zipWithIndex
      index.map((x:(String,Int))=>{
        var temp=0
        temp= x._2+1
        (temp,x._1.split(" ").size)
      })
    }
    def exam9_2(path:String)={
      val bufferedSource: BufferedSource = Source.fromFile(path)
      val lines: Iterator[String] = bufferedSource.getLines()
     // ("word","word")
      val words: Iterator[String] = lines.flatMap(x=>x.split(" "))
      val filtered: Iterator[String] = words.filter(_ !=" ")
      //(key,List(word))
      val grouped: Map[String, List[String]] = filtered.toList.groupBy((x:String)=>x)
      //(key,count)
      val mapped: Map[String, Int] = grouped.mapValues(_.size)
      val sorted: List[(String, Int)] = mapped.toList.sortBy(_._2).reverse
      val count = sorted(0)._2
      val res0: List[(String, Int)] = sorted.filter((x:(String,Int))=>x._2 == count)
      res0.foreach((x:(String,Int))=>println(x._1))
    }
  }

}
