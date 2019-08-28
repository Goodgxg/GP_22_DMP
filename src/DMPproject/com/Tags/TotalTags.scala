package com.Tags

import com.utils.{GetSparkUtils, TagUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TotalTags {
  def main(args: Array[String]): Unit = {
    if (args.length != 4)
      sys.exit()
    val Array(dicpath, keypath, inpath, outpath) = args

    val spark: SparkSession = GetSparkUtils.getSpark(this.getClass.getName)
    val sc = spark.sparkContext
    val keyRDD = spark.read.textFile(keypath).rdd
    val dicRDD = spark.read.textFile(dicpath).rdd
    val dicMap = dicRDD.map(_.split("\t")).filter(_.size >= 5)
      .map(x => {
        (x(4), x(1))
      }).collectAsMap()

    val kwlist: List[String] = dicRDD.collect().toList

    val broad = sc.broadcast(kwlist)
    val nameMap = sc.broadcast(dicMap)


    val dataRDD = spark.read.parquet(inpath).filter(TagUtils.oneUserId).rdd
    val res: RDD[(String, List[(String, Int)])] = dataRDD.map(row => {
      val id = TagUtils.getOneUserId(row)
      val adTag = TagsAd.makeTags(row)
      val KeyTag: List[(String, Int)] = KeyWordsTrait.makeTags(row, broad)
      val listDevice: List[(String, Int)] = DeviceTagsTrait.makeTags(row)
      val proviceList: List[(String, Int)] = ProvinceCityTrait.makeTags(row)
      val ProAd: List[(String, Int)] = ProAdTagsTrait.makeTags(row)
      val appTag: List[(String, Int)] = AppTrait.makeTags(row, nameMap)
      (id, adTag ::: KeyTag ::: listDevice ::: proviceList ::: ProAd ::: appTag)
    })
    val result = res.reduceByKey((list1, list2) => {
      (list1 ::: list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_ + _._2)).toList
    })

    result.saveAsTextFile(outpath)
    spark.stop()

  }
}
