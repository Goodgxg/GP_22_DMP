package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row


object DeviceTagsTrait extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    var row = args(0).asInstanceOf[Row]
    //client: Int,	设备类型 （1：android 2：ios 3：wp）
    val client = row.getAs[Int]("client")
    //联网方式
    val networkmannername = row.getAs[String]("networkmannername")
    //
    val ispname = row.getAs[String]("ispname")

    client match {
      case 1 => list:+=("1 Android D00010001",1)
      case 2 => list:+=("2 IOS D00010002",2)
      case 3 => list:+=("3 WinPhone",1)
      case _ => list:+=("_其他 D00010004",1)
    }
    networkmannername match {
      case "wifi" => list:+=("WIFI D00020001",1)
      case "4G" =>list:+=("4G D0020002",1)
      case "3G" =>list:+=("3G D0020003",1)
      case "2G" =>list:+=("2G D00020003",1)
      case _ =>list:+=("_ D00020005",1)
    }
    list:+=(ispname,1)

    list
  }
}
