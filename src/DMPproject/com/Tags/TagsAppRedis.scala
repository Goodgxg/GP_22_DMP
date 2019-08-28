//package com.Tags
//
//import com.utils.{GetSparkUtils, LogicUtils, TagUtils, Write2RedisUtils}
//import org.apache.commons.lang3.StringUtils
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import redis.clients.jedis.{Jedis, JedisPool}
//
//object TagsAppRedis {
//  def main(args: Array[String]): Unit = {
//    if (args.length != 3) {
//      sys.exit()
//    }
//    val Array(dicPath, inputPath, outputPath) = args
//    //    val spark: SparkSession = GetSparkUtils.getSpark(this.getClass.getName)
//
//    val conf: SparkConf = new SparkConf()
//      .setAppName(this.getClass.getName)
//      .setMaster("local[*]")
//
//    val spark = SparkSession.builder()
//      .config(conf)
//      .getOrCreate()
//    val dataDF = spark.read.parquet(inputPath)
//    val dataRDD = dataDF.filter(TagUtils.oneUserId).rdd
//
////    val sc = spark.sparkContext
//
////        val dicRDD = sc.textFile(dicPath)
////    val dicRDD: RDD[String] = spark.read.textFile(dicPath).rdd
////    val idAndName: RDD[(String, String)] = dicRDD.map(_.split("\t")).filter(_.size >= 5)
////      .map(x => {
////        (x(4), x(1))
////      })
////    val dicMap = idAndName.collectAsMap()
////
////    val pool = new JedisPool("hadoop02",6379)
////    val jedis = pool.getResource
////    jedis.hmset("dic",dicMap)
//
//    val appRes: RDD[(String, List[(String, Int)])] = dataRDD.map(row => {
//      val userid = TagUtils.getOneUserId(row)
//      val list = AppTrait.makeTags(row)
//      (userid, list)
//    })
//
//    appRes.saveAsTextFile(outputPath)
//    spark.stop()
//
//
//  }
//
//}
