package com.analyze

import com.caseclass.DataCase
import com.utils.{GetSparkUtils, LogicUtils, SaveJDBCUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object DeviceType {
  def main(args: Array[String]): Unit = {
    if(args.length!=1)
      sys.exit()
    val Array(inputpath) = args
    val spark = GetSparkUtils.getSpark(this.getClass.getName)
    spark.read.parquet(inputpath)
    val dataDF: DataFrame = spark.read.parquet(inputpath)
    val dataRdd: RDD[Row] = dataDF.rdd
    val lineData: RDD[(String,List[Double])] = dataRdd.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val devicetype = row.getAs[Int]("devicetype")
      var devicename = ""
      devicetype match {
        case 1=>{
          devicename="手机"
        }
        case 2=>{
          devicename = "平板"
        }
        case _=>{
          devicename = "其他"
        }
      }
      // 创建三个对应的方法处理九个指标
      (devicename, LogicUtils.request(requestmode, processnode) ++ LogicUtils.click(requestmode, iseffective) ++ LogicUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
    })
    val result: RDD[(String, List[Double])] = lineData.reduceByKey((x, y) => {
      val tuples: List[(Double, Double)] = x.zip(y)
      tuples.map(x => {
        x._1 + x._2
      })
    })
    import spark.implicits._
    val saveDF = result.map(x => {
      DataCase(x._1, x._2(0), x._2(1), x._2(2), x._2(5), x._2(6), x._2(3), x._2(4), x._2(7), x._2(8))
    }).toDF()
    SaveJDBCUtils.saveDataToMysql(saveDF,"t_devie")
    spark.stop()

  }
}
