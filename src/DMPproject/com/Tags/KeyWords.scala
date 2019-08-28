package com.Tags

import com.utils.{GetSparkUtils, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object KeyWords {
  def main(args: Array[String]): Unit = {
    if(args.length!=3)
      sys.exit()
    val Array(dicpath,inpath,outpath) = args

    val spark: SparkSession = GetSparkUtils.getSpark(this.getClass.getName)
    val dicRDD = spark.read.textFile(dicpath).rdd

    //    val dicMap = dicRDD.map(_.split("\t")).filter(_.size >= 5)
    //      .map(x => {
    //        (x(4), x(1))
    //      }).collectAsMap()
        val kwlist: List[String] = dicRDD.collect().toList
    val broad = spark.sparkContext.broadcast(kwlist)


    val dataRDD = spark.read.parquet(inpath).filter(TagUtils.oneUserId).rdd
    val resKW = dataRDD.map(row => {
      val id = TagUtils.getOneUserId(row)
      val list: List[(String, Int)] = KeyWordsTrait.makeTags(row, broad)


      (id, list)

    })
    resKW.saveAsTextFile(outpath)
    spark.stop()
  }

}
