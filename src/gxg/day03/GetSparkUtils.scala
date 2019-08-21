package com.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetSparkUtils {
  def getEnablHiveSupport(appname:String):SparkSession={
    val conf = new SparkConf().setAppName(appname).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
  def getSpark(appname:String):SparkSession={
    val conf = new SparkConf().setAppName(appname)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "snappy")
      .setMaster("local[*]")
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()
    spark
  }

}
