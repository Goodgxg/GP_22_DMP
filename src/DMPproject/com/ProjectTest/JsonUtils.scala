package com.ProjectTest

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils

object JsonUtils {
  def getBusinesList(strJson:String):List[(String,Int)]={
    var list = List[(String,Int)]()

    val dataJson = JSON.parseObject(strJson)
    if(dataJson.getIntValue("status")==0){
      return list
    }
    val regJson: JSONObject = dataJson.getJSONObject("regeocode")
    if(regJson.isEmpty){
      return list
    }

    val arrPoisJson: JSONArray = regJson.getJSONArray("pois")

    for(item <- arrPoisJson.toArray){
      if(item.isInstanceOf[JSONObject]){
        val itemJson = item.asInstanceOf[JSONObject]
        val busValue: String = itemJson.getString("businessarea")
        if(StringUtils.isNotBlank(busValue) && busValue.length>=1){
          list:+=(busValue,1)
        }
      }
    }
    list
  }

  def typeTags(strJson:String):List[(String,Int)]={
    var list = List[(String,Int)]()

    val dataJson = JSON.parseObject(strJson)
    if(dataJson.getIntValue("status")==0){
      list:+=("",0)
      return list
    }
    val regJson: JSONObject = dataJson.getJSONObject("regeocode")
    if(regJson.isEmpty){
      list:+=("",0)
      return list
    }

    val arrPoisJson: JSONArray = regJson.getJSONArray("pois")

    for(item <- arrPoisJson.toArray){
      if(item.isInstanceOf[JSONObject]){
        val itemJson = item.asInstanceOf[JSONObject]
        val typeValue = itemJson.getString("type")
        if(StringUtils.isNoneBlank(typeValue)){
          val typeStr: Array[String] = typeValue.split(";")
          for(s<-typeStr)
          list:+=("type:"+s,1)
        }
      }
    }
    list
  }

}
