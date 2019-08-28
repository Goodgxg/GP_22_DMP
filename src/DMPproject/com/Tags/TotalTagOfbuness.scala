package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.{GetSparkUtils, TagUtils}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TotalTagOfbuness {
  def main(args: Array[String]): Unit = {
    if (args.length != 5)
      sys.exit()
    val Array(dicpath, keypath, inpath, outpath,days) = args

    val spark: SparkSession = GetSparkUtils.getSpark(this.getClass.getName)
    val sc = spark.sparkContext
    val keyRDD = spark.read.textFile(keypath).rdd
    val dicRDD = spark.read.textFile(dicpath).rdd
    val dicMap = dicRDD.map(_.split("\t")).filter(_.size >= 5)
      .map(x => {
        (x(4), x(1))
      }).collectAsMap()

    val kwlist: List[String] = dicRDD.collect().toList

    val broad = sc.broadcast(kwlist)
    val nameMap = sc.broadcast(dicMap)


    val dataRDD = spark.read.parquet(inpath).filter(TagUtils.oneUserId).rdd
    val res: RDD[(String, List[(String, Int)])] = dataRDD.map(row => {
      val id = TagUtils.getOneUserId(row)
      val adTag = TagsAd.makeTags(row)
      val KeyTag: List[(String, Int)] = KeyWordsTrait.makeTags(row, broad)
      val listDevice: List[(String, Int)] = DeviceTagsTrait.makeTags(row)
      val proviceList: List[(String, Int)] = ProvinceCityTrait.makeTags(row)
      val ProAd: List[(String, Int)] = ProAdTagsTrait.makeTags(row)
      val appTag: List[(String, Int)] = AppTrait.makeTags(row, nameMap)
//      var business: List[(String, Int)] = BusinessTag.makeTags(row)

//      if(business.size==0){
//        business = Nil
//      }
      (id, adTag ::: KeyTag ::: listDevice ::: proviceList ::: ProAd ::: appTag )
    })
    val totalTags = res.reduceByKey((list1, list2) => {
      (list1 ::: list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_ + _._2)).toList
    })
    save2HbaseTable(days,sc,totalTags)
//    totalTags.saveAsTextFile(outpath)
    spark.stop()
  }
  def save2HbaseTable(days:String,sc:SparkContext,totalTags:RDD[(String, List[(String, Int)])]): Unit ={
    val load = ConfigFactory.load("application.conf")
    val tableName = load.getString("hbase.TableName")
    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181")
//    val configuration = sc.hadoopConfiguration
//    conf.set("hbase.zookeeper.quorum",load.getString("hbase.quorum"))
//    conf.set("hbase.rootdir","hdfs://hadoop01:9000/hbase")
    //创建hbaseConnection
    val connection = ConnectionFactory.createConnection(conf)
    val hadmin = connection.getAdmin
//    val namespaceDes = NamespaceDescriptor.create("GP").build()
//    hadmin.createNamespace(namespaceDes)
    if(!hadmin.tableExists(TableName.valueOf(tableName))){
      //创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hadmin.createTable(tableDescriptor)
    }
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    totalTags.map{
      case(userid,userTag)=>{
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(days),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)
  }
}
