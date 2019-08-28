package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.{GetSparkUtils, TagUtils}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TotalTagDGraph {
  def main(args: Array[String]): Unit = {
    if (args.length != 5)
      sys.exit()
    val Array(dicpath, keypath, inpath, outpath, days) = args

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
      .map(row => {
        val userList: List[String] = TagUtils.getAllUserId(row)
        (userList, row)
      })

    //构建点集合
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = dataRDD.flatMap(tp => {
      val row = tp._2
      val adTag = TagsAd.makeTags(row)
      val KeyTag: List[(String, Int)] = KeyWordsTrait.makeTags(row, broad)
      val listDevice: List[(String, Int)] = DeviceTagsTrait.makeTags(row)
      val proviceList: List[(String, Int)] = ProvinceCityTrait.makeTags(row)
      val ProAd: List[(String, Int)] = ProAdTagsTrait.makeTags(row)
      val appTag: List[(String, Int)] = AppTrait.makeTags(row, nameMap)
      val busTag: List[(String, Int)] = BusinessTag.makeTags(row)
      val allTag: List[(String, Int)] = busTag ++ adTag ++ KeyTag ++ listDevice ++ proviceList ++ ProAd ++ appTag
      //保证其中一个点携带者所有的标签,同时也保留所有的userid
      val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ allTag
      //处理所有的点的集合
      val Verties: List[(Long, List[(String, Int)])] = tp._1.map(uId => {
        //保证一个点携带标签
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, Nil)
        }
      })
      Verties
    })
    //    vertiesRDD.take(50).foreach(println)
    val edge: RDD[Edge[Long]] = dataRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode.toLong, uId.hashCode.toLong, 0L))
    })
    //构建图
    val graph: Graph[List[(String, Int)], Long] = Graph(vertiesRDD, edge)
    val vertices = graph.connectedComponents().vertices
    val result: RDD[(VertexId, List[(String, Int)])] = vertices.join(vertiesRDD).map {
      case (uId, (conId, tagsAll)) => (conId, tagsAll)
    }.reduceByKey((list1, list2) => {
      (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
    save2HbaseAlltags(days,sc, result)
    //    totalTags.saveAsTextFile(outpath)
    spark.stop()
  }

  def save2HbaseAlltags(days: String, sc: SparkContext, totalTags: RDD[(VertexId, List[(String, Int)])]): Unit = {
    //    val load = ConfigFactory.load("application.conf")
    val tableName = "allTags"
    //    val conf = HBaseConfiguration.create()
    //        conf.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181")
    val conf = sc.hadoopConfiguration
    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //    conf.set("hbase.rootdir","hdfs://hadoop01:9000/hbase")
    //创建hbaseConnection
    val connection = ConnectionFactory.createConnection(conf)
    val hadmin = connection.getAdmin
    //    val namespaceDes = NamespaceDescriptor.create("GP").build()
    //    hadmin.createNamespace(namespaceDes)
    if (!hadmin.tableExists(TableName.valueOf(tableName))) {
      //创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hadmin.createTable(tableDescriptor)
    }
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    totalTags.map {
      case (userid, userTag) => {
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTag.map(t => t._1 + "," + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(days), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }.saveAsHadoopDataset(jobConf)
  }

  def save2HbaseTable(days: String, sc: SparkContext, totalTags: RDD[(String, List[(String, Int)])]): Unit = {
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
    if (!hadmin.tableExists(TableName.valueOf(tableName))) {
      //创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hadmin.createTable(tableDescriptor)
    }
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    totalTags.map {
      case (userid, userTag) => {
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTag.map(t => t._1 + "," + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(days), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }.saveAsHadoopDataset(jobConf)
  }
}
