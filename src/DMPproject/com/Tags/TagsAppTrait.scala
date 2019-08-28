//package com.Tags
//
//import com.utils.Tag
//import org.apache.commons.lang3.StringUtils
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Row, SparkSession}
//
//object TagsAppTrait {
//  /**
//    * 打标签统一接口
//    */
//  def makeTags(dicPath:String,args: Any*): List[(String, Int)] = {
//
//    val row = args(0).asInstanceOf[Row]
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val dicRDD: RDD[String] = sc.textFile(dicPath)
//    val idAndName: RDD[(String, String)] = dicRDD.map(_.split("\t")).filter(_.size >= 5)
//      .map(x => {
//        (x(4), x(1))
//      })
//val cacheRDD = idAndName.cache()
//
//    val appname = row.getAs[String]("appname");
//    val appid = row.getAs[String]("appid")
//    val tuples: Array[(String, String)] = cacheRDD.filter(_._1.equals(appid)).take(1)
//   val dicAppname = tuples.head._2
//    appname match {
//      case v if StringUtils.isNotBlank(v) => List(("APP"+v,1))
//      case v if StringUtils.isNotBlank(dicAppname) =>List(("APP"+dicAppname,1))
//      case _ =>List(("APP未知",1))
//    }
//  }
//}
