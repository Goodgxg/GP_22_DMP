package com.ProjectTest

import com.utils.GetSparkUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

object JsonAnalyze {
  def main(args: Array[String]): Unit = {
    if(args.length!=3)
      sys.exit()
    val Array(inPath,outPathbus,outPathtag) = args

    val spark = GetSparkUtils.getSpark(this.getClass.getName)
    val dataRDD: RDD[String] = spark.read.textFile(inPath).rdd

    val busRDD: RDD[List[(String, Int)]] = dataRDD.map(JsonUtils.getBusinesList(_))
    val result = busRDD.map(x => {
      x.groupBy(_._1).map(x => (x._1, x._2.size)).toList
    })
    val busResult: RDD[List[(String, Int)]] = result.filter(!_(0)._1.equals("[]"))
    val tagRDD: RDD[List[(String, Int)]] = dataRDD.map(JsonUtils.typeTags(_))
    val tagRes: RDD[List[(String, Int)]] = tagRDD.map(x => {
      x.filter(_._1.size > 0).groupBy(_._1).map(x => (x._1, x._2.size)).toList
    })
//    result.saveAsTextFile(outPathbus)
//    tagRes.saveAsTextFile(outPathtag)
    println(busResult.collect.toBuffer)
    println(tagRes.collect.toBuffer)

    spark.stop()
  }

}
