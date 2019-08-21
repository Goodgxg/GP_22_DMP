package com.ETL

import com.utils.{CheckArgsUtil, GetSparkUtils, SaveJDBCUtils}
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
      ((pro, city), request(requestmode, processnode) ++ click(requestmode, iseffective) ++ Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
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
    SaveJDBCUtils.saveDataToMysql(saveDF,"t_target")
    spark.stop()
  }

  def checkArgs(args: Array[String]): Boolean = {
    if (args.length == 2) true
    else false
  }


  // 此方法处理请求数
  def request(requestmode: Int, processnode: Int): List[Double] = {
    var (a, b, c) = (0.0, 0.0, 0.0)
    if (requestmode == 1 && processnode >= 1)
      a = 1.0
    if (requestmode == 1 && processnode >= 2)
      b = 1.0
    if (requestmode == 1 && processnode == 3)
      c = 1.0
    List(a, b, c)
  }

  // 此方法处理展示点击数

  def click(requestmode: Int, iseffective: Int): List[Double] = {
    var (a, b) = (0.0, 0.0)
    if (requestmode == 2 && iseffective == 1)
      a = 1.0
    if (requestmode == 3 && iseffective == 1)
      b = 1.0
    List(a, b)
  }

  // 此方法处理竞价操作

  def Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, WinPrice: Double, adpayment: Double): List[Double] = {
    var (a, b, c, d) = (0.0, 0.0, 0.0, 0.0)
    if (iseffective == 1 && isbilling == 1 && isbid == 1)
      a = 1.0
    if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0)
      b = 1.0
    if (iseffective == 1 && isbilling == 1 && iswin == 1)
      c = WinPrice / 1000
    if (iseffective == 1 && isbilling == 1 && iswin == 1)
      d = adpayment / 1000
    List(a, b, c, d)
  }

}
case class ResLineData(
                     provincename:String,
                     cityname:String,
                     ori_request:Double,
                     eff_request:Double,
                     ad_request:Double,
                     bid_num:Double,
                     succ_bid:Double,
                     terminal:Double,
                     click:Double,
                     winprice:Double,
                     adpayment:Double
                   )