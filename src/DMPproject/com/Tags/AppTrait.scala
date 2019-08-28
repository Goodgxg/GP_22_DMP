package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppTrait extends Tag{
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val nameMap = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    var appname = row.getAs[String]("appname");
    val appid = row.getAs[String]("appid")
    if (!StringUtils.isNotEmpty(appname)&&StringUtils.isNotBlank(appid)) {
      appname = nameMap.value.getOrElse(appid,"未知")
      list :+= ("APP" + appname, 1)
    } else {
      list :+= (appname, 1)
    }
    list
  }
}
