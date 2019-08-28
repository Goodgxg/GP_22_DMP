package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.immutable._


object TagsAd extends Tag{
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取广告类型,广告名称.
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v>9 =>  list:+=("LC"+v,1)
      case v if v<=9 && v>0 => list:+=("LC0"+v,1)
    }
    val adname = row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adname)){
      list:+=("LN"+adname,1)
    }
    list
  }
}
