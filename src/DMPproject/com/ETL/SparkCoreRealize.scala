package com.ETL

import com.utils.{CheckArgsUtil, GetSparkUtils, LogicUtils, SaveJDBCUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}


object SparkCoreRealize {
  def main(args: Array[String]): Unit = {
    val spark = GetSparkUtils.getSpark(this.getClass.getName)
    if (!checkArgs(args))
      sys.exit()
    val Array(inputpath, outputpath) = args

    val dataDF: DataFrame = spark.read.parquet(inputpath)
    val dataRdd: RDD[Row] = dataDF.rdd
    val lineData: RDD[((String, String), List[Double])] = dataRdd.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标
      ((pro, city), LogicUtils.request(requestmode, processnode) ++ LogicUtils.click(requestmode, iseffective) ++ LogicUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
    })
    val result: RDD[((String, String), List[Double])] = lineData.reduceByKey((x, y) => {
      val tuples: List[(Double, Double)] = x.zip(y)
      tuples.map(x => {
        x._1 + x._2
      })
    })
    import spark.implicits._
    val saveDF: DataFrame = result.map(x => {
      ResLineData(x._1._1, x._1._2, x._2(0), x._2(1), x._2(2), x._2(5), x._2(6), x._2(3), x._2(4), x._2(7), x._2(8))
    }).toDF()
    SaveJDBCUtils.saveDataToMysql(saveDF, "t_target")
    spark.stop()
  }

  def checkArgs(args: Array[String]): Boolean = {
    if (args.length == 2) true
    else false
  }
}

case class ResLineData(
                        provincename: String,
                        cityname: String,
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