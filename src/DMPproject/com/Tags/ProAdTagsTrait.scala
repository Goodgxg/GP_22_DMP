package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object ProAdTagsTrait extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    val row = args(0).asInstanceOf[Row]
    val adid = row.getAs[Int]("adplatformproviderid")
    List(("CN" + adid, 1))
  }
}
