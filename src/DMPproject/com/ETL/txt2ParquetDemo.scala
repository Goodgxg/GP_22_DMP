package com.ETL

import java.util.Properties

import com.utils.{SaveJDBCUtils, Utils2Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object txt2ParquetDemo {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      print("目录不正确")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val lines: RDD[String] = sc.textFile(inputPath)
    val rowData: RDD[LineData] = lines.map(_.split(",", -1)).filter(_.length >= 85)
      .map(arr => LineData(
        arr(0),
        Utils2Type.toInt(arr(1)),
        Utils2Type.toInt(arr(2)),
        Utils2Type.toInt(arr(3)),
        Utils2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        Utils2Type.toInt(arr(7)),
        Utils2Type.toInt(arr(8)),
        Utils2Type.toDouble(arr(9)),
        Utils2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        Utils2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        Utils2Type.toInt(arr(20)),
        Utils2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        Utils2Type.toInt(arr(26)),
        arr(27),
        Utils2Type.toInt(arr(28)),
        arr(29),
        Utils2Type.toInt(arr(30)),
        Utils2Type.toInt(arr(31)),
        Utils2Type.toInt(arr(32)),
        arr(33),
        Utils2Type.toInt(arr(34)),
        Utils2Type.toInt(arr(35)),
        Utils2Type.toInt(arr(36)),
        arr(37),
        Utils2Type.toInt(arr(38)),
        Utils2Type.toInt(arr(39)),
        Utils2Type.toDouble(arr(40)),
        Utils2Type.toDouble(arr(41)),
        Utils2Type.toInt(arr(42)),
        arr(43),
        Utils2Type.toDouble(arr(44)),
        Utils2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        Utils2Type.toInt(arr(57)),
        Utils2Type.toDouble(arr(58)),
        Utils2Type.toInt(arr(59)),
        Utils2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        Utils2Type.toInt(arr(73)),
        Utils2Type.toDouble(arr(74)),
        Utils2Type.toDouble(arr(75)),
        Utils2Type.toDouble(arr(76)),
        Utils2Type.toDouble(arr(77)),
        Utils2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        Utils2Type.toInt(arr(84))
      ))
    import spark.implicits._
    val dataFrame = rowData.toDF()

    dataFrame.createOrReplaceTempView("t_test")
    val resultDF: DataFrame = spark.sql("select long_fix,lat from t_test")
    SaveJDBCUtils.saveDataToMysql(resultDF,"longandlat")
    //保存到数据库
    //    SaveMysql(dataFrame, spark)
    //用sparkcore实现并保存到磁盘(json格式)
//    val seqData: RDD[SeqResult] = SparkCore(rowData, outputPath)
//    val seqRes = seqData.toDF()

//    seqRes.write.json(outputPath)
//    seqRes.write.partitionBy("provincename","cityname").parquet(outputPath)
//        dataFrame.coalesce(1).write.parquet(outputPath)

    spark.stop()
    sc.stop()
  }

  def SaveMysql(dataFrame: DataFrame, spark: SparkSession): Unit = {
    //创建临时表,统计数据
    dataFrame.createOrReplaceTempView("t_tmp")
    val result: DataFrame = spark.sql("select count(1) ct,provincename,cityname from t_tmp group by provincename,cityname")
    //以properties的方式和数据库进行连接并存储到MySQL数据库
    val pro = new Properties()
    pro.put("user", "root")
    pro.put("password", "123456")
    val url = "jdbc:mysql://localhost/dmp"
    result.write.mode(SaveMode.Append).jdbc(url, "t_result", pro)
  }

  def SparkCore(data: RDD[LineData], outputPath: String): RDD[SeqResult] = {
    val tupData: RDD[(String, Int)] = data.map(line => {
      (line.provincename + "_" + line.cityname, 1)
    })
    val result = tupData.reduceByKey(_ + _)
    val saveData = result.map(data => {
      val proAndCity = data._1.split("_")
      new SeqResult(proAndCity(0), proAndCity(1), data._2)
    })
    saveData

  }
}

case class SeqResult(provincename: String, cityname: String, ct: Int)

case class LineData(sessionid: String,
                    advertisersid: Int,
                    adorderid: Int,
                    adcreativeid: Int,
                    adplatformproviderid: Int,
                    sdkversion: String,
                    adplatformkey: String,
                    putinmodeltype: Int,
                    requestmode: Int,
                    adprice: Double,
                    adppprice: Double,
                    requestdate: String,
                    ip: String,
                    appid: String,
                    appname: String,
                    uuid: String,
                    device: String,
                    client: Int,
                    osversion: String,
                    density: String,
                    pw: Int,
                    ph: Int,
                    long_fix: String,
                    lat: String,
                    provincename: String,
                    cityname: String,
                    ispid: Int,
                    ispname: String,
                    networkmannerid: Int,
                    networkmannername: String,
                    iseffective: Int,
                    isbilling: Int,
                    adspacetype: Int,
                    adspacetypename: String,
                    devicetype: Int,
                    processnode: Int,
                    apptype: Int,
                    district: String,
                    paymode: Int,
                    isbid: Int,
                    bidprice: Double,
                    winprice: Double,
                    iswin: Int,
                    cur: String,
                    rate: Double,
                    cnywinprice: Double,
                    imei: String,
                    mac: String,
                    idfa: String,
                    openudid: String,
                    androidid: String,
                    rtbprovince: String,
                    rtbcity: String,
                    rtbdistrict: String,
                    rtbstreet: String,
                    storeurl: String,
                    realip: String,
                    isqualityapp: Int,
                    bidfloor: Double,
                    aw: Int,
                    ah: Int,
                    imeimd5: String,
                    macmd5: String,
                    idfamd5: String,
                    openudidmd5: String,
                    androididmd5: String,
                    imeisha1: String,
                    macsha1: String,
                    idfasha1: String,
                    openudidsha1: String,
                    androididsha1: String,
                    uuidunknow: String,
                    userid: String,
                    iptype: Int,
                    initbidprice: Double,
                    adpayment: Double,
                    agentrate: Double,
                    lomarkrate: Double,
                    adxrate: Double,
                    title: String,
                    keywords: String,
                    tagid: String,
                    callbackdate: String,
                    channelid: String,
                    mediatype: Int
                   )