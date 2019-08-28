package com.ProjectTest

import com.Tags.{BusinessTag, DeviceTagsTrait}
import com.utils.{GetSparkUtils, TagUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object  BuninessTest {
  def main(args: Array[String]): Unit = {
    if(args.length!=2)
      sys.exit()
    val Array(inpath,outpath) = args

    val spark: SparkSession = GetSparkUtils.getSpark(this.getClass.getName)
    val dataRDD: RDD[Row] = spark.read.parquet(inpath).rdd
    dataRDD.collect.foreach(row=>{
      var business: List[(String, Int)] = BusinessTag.makeTags(row)
    })
//    val resProid = dataRDD.map(row => {
//      val long = row.getAs[String]("long_fix").toDouble
//      val lat = row.getAs[String]("lat").toDouble
//      (long,lat)
//
//    }).filter(_._1>10)
//    println(resProid.collect().toBuffer)
//    resProid.saveAsTextFile(outpath)
    spark.stop()
  }

}
