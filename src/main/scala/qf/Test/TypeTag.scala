package qf.Test

import qf.WeekOfProject.Project1.util.Tag
import com.alibaba.fastjson.{JSON, JSONObject}

object TypeTag extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val jsonparse = JSON.parseObject(args(0).toString)
    val status: Int = jsonparse.getIntValue("status")
    if(status==0) return list
    val regeocode=jsonparse.getJSONObject("regeocode")

    if(regeocode==null || regeocode.keySet().isEmpty) return list

    val pois= regeocode.getJSONArray("pois")

    if(pois==null || pois.isEmpty) return list

    for(item <- pois.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val arrType: Array[String] = json.getString("type").split(";")
        arrType.foreach(x=>{
          list:+=(x,1)
        })
      }
    }
    list
  }
}
