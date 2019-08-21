package com.ETL

import com.utils.GetSparkUtils
import org.apache.spark.sql.DataFrame

object SparkSqlRealize {
  def main(args: Array[String]): Unit = {
    if(!checkArgs(args))
      sys.exit()
    val Array(inputpath)=args
    val spark = GetSparkUtils.getSpark(this.getClass.getName)
    val dataDF: DataFrame = spark.read.parquet(inputpath)
    dataDF.createOrReplaceTempView("t_tmp")
    val resultDF: DataFrame = spark.sql(
      """
        |select
        |provincename,
        |cityname,
        |sum(case when requestmode=1 and processnode >= 1 then 1 else 0 end) ori_request,
        |sum(case when requestmode=1 and processnode >= 2 then 1 else 0 end) eff_request,
        |sum(case when requestmode=1 and processnode = 3 then 1 else 0 end) ad_request,
        |sum(case when iseffective=1 and isbilling = 1 and isbid = 1 then 1 else 0 end) bid_num,
        |sum(case when iseffective=1 and isbilling = 1 and iswin = 1 and adorderid <> 0 then 1 else 0 end) succ_bid,
        |sum(case when requestmode=2 and iseffective = 1 then 1 else 0 end) terminal,
        |sum(case when requestmode=3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective=1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) winprice,
        |sum(case when iseffective=1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) adpayment
        |from t_tmp
        |group by
        |provincename,
        |cityname
      """.stripMargin)
//    resultDF.show()
    spark.stop()
  }
def checkArgs(args:Array[String]):Boolean={
if(args.length==1) true
else false
}
}
