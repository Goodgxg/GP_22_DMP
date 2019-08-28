package com.Tags

import com.utils.{GetSparkUtils, TagUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("参数不对,退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    //创建上下文
    val spark: SparkSession = GetSparkUtils.getSpark(this.getClass.getName)
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
//    val spark = SparkSession.builder().config(conf).getOrCreate()
    //读取数据
    val dataDF: DataFrame = spark.read.parquet(inputPath)
          val dataRDD: RDD[Row] = dataDF.filter(TagUtils.oneUserId).rdd
    val userTags: RDD[(String, List[(String, Int)])] = dataRDD.map(row => {
      val userid = TagUtils.getOneUserId(row)
      val tags: List[(String, Int)] = TagsAd.makeTags(row)
      (userid, tags)
    })
    userTags.saveAsTextFile(outputPath)
    spark.stop()
  }

}
