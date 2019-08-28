package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * 商圈解析工具
  */
object AmapUtil {
  //获取高德地图商圈信息
  def getBusinessFromAmap(long: Double, lat: Double): String = {
    val location = long + "," + lat
    val urlStr = s"https://restapi.amap.com/v3/geocode/regeo?location=$location&key=46b062706e82f9a4eb5b37eaf93d802d&radius=1000"
    val str: String = HttpUtils.get(urlStr)
    val jsonparse: JSONObject = JSON.parseObject(str)
    val status: Int = jsonparse.getIntValue("status")
    if (status == 0)
      return ""
    val regeJson: JSONObject = jsonparse.getJSONObject("regeocode")
    val addJson: JSONObject = regeJson.getJSONObject("addressComponent")
    val jsonArray: JSONArray = addJson.getJSONArray("businessAreas")
    val buffer: ListBuffer[String] = collection.mutable.ListBuffer[String]()

    for (item <- jsonArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
