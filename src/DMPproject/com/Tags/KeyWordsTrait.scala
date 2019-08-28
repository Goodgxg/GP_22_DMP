package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object KeyWordsTrait extends Tag{
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[List[String]]]
    val kwds = row.getAs[String]("keywords").split("\\|")
    kwds.filter(word=>{
      word.length>=3 && word.length<=8 && !stopword.value.contains(word)
    }).foreach(word => list:+=("K"+word,1))
    list
  }
}
