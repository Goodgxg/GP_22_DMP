package com.analyze



import java.sql.{Connection, DriverManager, PreparedStatement}

import com.caseclass.DataCase
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.utils.{GetSparkUtils, LogicUtils, SaveJDBCUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * <dependency>
  * <groupId>c3p0</groupId>
  * <artifactId>c3p0</artifactId>
  * <version>0.9.1.2</version>
  * </dependency>
  */


object OSType {


  def main(args: Array[String]): Unit = {
    if (args.length != 1)
      sys.exit()
    val Array(inputpath) = args
    val spark = GetSparkUtils.getSpark(this.getClass.getName)
    spark.read.parquet(inputpath)
    val dataDF: DataFrame = spark.read.parquet(inputpath)
    val dataRdd: RDD[Row] = dataDF.rdd
    val lineData: RDD[(String, List[Double])] = dataRdd.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val client = row.getAs[Int]("client")
      var osType = ""
      client match {
        case 1 => {
          osType = "android"
        }
        case 2 => {
          osType = "ios"
        }
        case 3 => {
          osType = "wp"
        }
        case _ => {
          osType = "未知"
        }
      }
      (osType, LogicUtils.request(requestmode, processnode) ++ LogicUtils.click(requestmode, iseffective) ++ LogicUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
    })
    val result: RDD[(String, List[Double])] = lineData.reduceByKey((x, y) => {
      val tuples: List[(Double, Double)] = x.zip(y)
      tuples.map(x => {
        x._1 + x._2
      })
    })
    //转成DF,实现数据库的写入(需要反射增加schema信息
    //    import spark.implicits._
    //    val saveDF = result.map(x => {
    //      DataCase(x._1, x._2(0), x._2(1), x._2(2), x._2(5), x._2(6), x._2(3), x._2(4), x._2(7), x._2(8))
    //    }).toDF()
    //    SaveJDBCUtils.saveDataToMysql(saveDF,"t_os")
    //rdd单连接实现数据库写入
    //      result.foreachPartition(part => {
    //      data2Mysql(part)
    //    })
    //c3p0实现数据库的写入
    result.foreachPartition(part => {
      val conn = getC3p0Data().getConnection
      conn.setAutoCommit(false)
      val pre: PreparedStatement = conn.prepareStatement("insert into t_foreach values(?,?,?,?,?,?,?,?,?,?)")
      part.foreach(x => {
        pre.setString(1, x._1)
        pre.setDouble(2, x._2(0))
        pre.setDouble(3, x._2(1))
        pre.setDouble(4, x._2(2))
        pre.setDouble(7, x._2(5))
        pre.setDouble(8, x._2(6))
        pre.setDouble(5, x._2(3))
        pre.setDouble(6, x._2(4))
        pre.setDouble(9, x._2(7))
        pre.setDouble(10, x._2(8))
        pre.addBatch()
      })
      try {
        pre.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        pre.close()
        conn.close()
      }
    })
    spark.stop()
  }

  //  def data2Mysql(part: Iterator[(String, List[Double])]): Unit = {
  //    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/dmp","root","123456")
  //    val pre: PreparedStatement = conn.prepareStatement("insert into t_foreach values(?,?,?,?,?,?,?,?,?,?)")
  //    part.foreach(x => {
  //      pre.setString(1, x._1)
  //      pre.setDouble(2, x._2(0))
  //      pre.setDouble(3, x._2(1))
  //      pre.setDouble(4, x._2(2))
  //      pre.setDouble(7, x._2(5))
  //      pre.setDouble(8, x._2(6))
  //      pre.setDouble(5, x._2(3))
  //      pre.setDouble(6, x._2(4))
  //      pre.setDouble(9, x._2(7))
  //      pre.setDouble(10, x._2(8))
  //      pre.execute()
  //    })
  //    pre.close()
  //    conn.close()
  //  }
  def getC3p0Data(): ComboPooledDataSource = {
    val source = new ComboPooledDataSource()
    source.setJdbcUrl("jdbc:mysql://localhost:3306/dmp")
    source.setDriverClass("com.mysql.jdbc.Driver")
    source.setUser("root")
    source.setPassword("123456")
    source.setMaxPoolSize(10)
    source.setMinPoolSize(5)
    source.setAcquireIncrement(3)
    source.setInitialPoolSize(2)
    source
  }


}
