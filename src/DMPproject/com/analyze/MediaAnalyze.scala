package com.analyze

import com.utils.{GetSparkUtils, LogicUtils, SaveJDBCUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MediaAnalyze {
  def main(args: Array[String]): Unit = {
    if(args.length!=2)
      sys.exit()
    val Array(dicpath,datapath) = args
    val conf =  new SparkConf().setAppName(this.getClass.getName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "snappy")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()
    val dicData: RDD[String] = sc.textFile(dicpath)
    val dicValue: Map[String, String] = dicData.map(_.split("\t")).filter(_.size >= 5)
      .map(x => {
        (x(4),x(1))
      }).collect().toMap
    val broad: Broadcast[Map[String, String]] = sc.broadcast(dicValue)
    val testBro: Map[String, String] = broad.value
    val logDF: DataFrame = spark.read.parquet(datapath)
    val logRdd: RDD[Row] = logDF.rdd
    val result: RDD[(String, List[Double])] = logRdd.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val appid = row.getAs[String]("appid")
      var appname = row.getAs[String]("appname")
      if (appname.length == 0) {
        appname = broad.value.getOrElse(appid,"未知设备")
      }
      (appname, LogicUtils.request(requestmode, processnode) ++ LogicUtils.click(requestmode, iseffective) ++ LogicUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
    })
      .reduceByKey((x, y) => {
        val tuples: List[(Double, Double)] = x.zip(y)
        tuples.map(x => {
          x._1 + x._2
        })
      })
    import spark.implicits._
    val saveRes = result.map(x => {
      MediaData(x._1, x._2(0), x._2(1), x._2(2), x._2(5), x._2(6), x._2(3), x._2(4), x._2(7), x._2(8))
    }).toDF
    SaveJDBCUtils.saveDataToMysql(saveRes,"t_media")
    spark.stop()
    sc.stop()
  }
}
case class MediaData(
                        appname: String,
                        ori_request: Double,
                        eff_request: Double,
                        ad_request: Double,
                        bid_num: Double,
                        succ_bid: Double,
                        terminal: Double,
                        click: Double,
                        winprice: Double,
                        adpayment: Double
                      )
