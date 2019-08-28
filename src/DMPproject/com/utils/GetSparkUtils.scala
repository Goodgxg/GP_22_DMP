package com.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object GetSparkUtils {

  def getEnablHiveSupport(appname: String): SparkSession = {
    val conf =  new SparkConf().setAppName(appname)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "snappy")
      .setMaster("local[*]")
    SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()

  }

  def getSpark(appname: String): SparkSession = {
    val conf =  new SparkConf().setAppName(appname)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "snappy")
      .setMaster("local[*]")
    SparkSession.builder().config(conf)
      .getOrCreate()

  }


}
