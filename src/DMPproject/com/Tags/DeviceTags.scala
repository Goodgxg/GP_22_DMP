package com.Tags

import com.utils.{GetSparkUtils, TagUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object DeviceTags {
  def main(args: Array[String]): Unit = {
    if(args.length!=2)
      sys.exit()
    val Array(inpath,outpath) = args

    val spark: SparkSession = GetSparkUtils.getSpark(this.getClass.getName)
    val dataRDD: RDD[Row] = spark.read.parquet(inpath).filter(TagUtils.oneUserId).rdd
    val resProid: RDD[(String, List[(String, Int)])] = dataRDD.map(row => {
      val id = TagUtils.getOneUserId(row)
      val list = DeviceTagsTrait.makeTags(row)
      (id, list)
    })
    resProid.saveAsTextFile(outpath)
    spark.stop()
  }
}
