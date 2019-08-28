package com.Tags

import com.utils.{GetSparkUtils, TagUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TagsApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 3)
      sys.exit()
    val Array(dicPath, inputPath, outputPath) = args
    //    val spark = GetSparkUtils.getSpark(this.getClass.getName)
    //    val dicRDD = spark.read.textFile(dicPath).rdd
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "snappy")
      .setMaster("local[*]")
//    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    val dicRDD: RDD[String] = sc.textFile(dicPath)
    val idAndName: RDD[(String, String)] = dicRDD.map(_.split("\t")).filter(_.size >= 5)
      .map(x => {
        (x(4), x(1))
      })
    val cacheRDD = idAndName.cache()
    val dataDF: DataFrame = spark.read.parquet(inputPath)
    val dataRDD = dataDF.filter(TagUtils.oneUserId).rdd
    val tagsRDD: RDD[(String, List[(String, Int)])] = dataRDD.map(row => {
      val appID = TagUtils.getOneUserId(row)
      var list = List[(String, Int)]()
      var appname = row.getAs[String]("appname");
      val appid = row.getAs[String]("appid")
      if (!StringUtils.isNotEmpty(appname)) {
        val tuples: Array[(String, String)] = cacheRDD.filter(_._1.equals(appid)).take(1)
        val appname = tuples.head._2
        appname match {
          case v if StringUtils.isNotBlank(appname) => list :+= ("APP" + appname, 1)
          case _ => list :+= ("APP未知", 1)
        }
      } else {
        list :+= (appname, 1)
      }
      //      val tags: List[(String, Int)] = TagsAppTrait.makeTags(dicPath,row)
      (appID, list)
    })

    tagsRDD.saveAsTextFile(outputPath)
    sc.stop()
    spark.stop()

  }

}
