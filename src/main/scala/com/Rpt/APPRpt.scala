package com.Rpt

import com.utils.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 媒体指标
  */
object APPRpt {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    // 判断路径是否正确
    if(args.length != 3){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath,dirPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df = sQLContext.read.parquet(inputPath)

    // 读取字典文件
    val map = sc.textFile(dirPath).map(_.split("\t",-1)).filter(_.length>=5)
      .map(arr=>(arr(4),arr(1))).collectAsMap()

    // 广播字典文件
    val broadcast = sc.broadcast(map)
    // 将数据进行匹配处理
    df.map(row=>{
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 获取媒体id和name
      var appname = row.getAs[String]("appname")
      if(!StringUtils.isNoneBlank(appname)){
        appname =broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      // 创建三个对应的方法处理九个指标
      val reqlist = RptUtils.request(requestmode,processnode)
      val clicklist = RptUtils.click(requestmode,iseffective)
      val adlist = RptUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment)
      (appname,reqlist++clicklist++adlist)
    })
      // 根据Key聚合Value
      .reduceByKey(
      (list1,list2)=>{
        // list(（1,1），（2,2），（3,3）)
        list1.zip(list2).map(t=>t._1+t._2)
      })
      // 整理元素
      .map(t=>{
      t._1+","+t._2.mkString(",")
    })
      .saveAsTextFile(outputPath)
  }
}
