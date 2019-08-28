package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import redis.clients.jedis.{Jedis, JedisPool}

object Write2RedisUtils {

def main(args: Array[String]): Unit = {
  val spark = GetSparkUtils.getSpark(this.getClass.getName)
  val dicRDD: RDD[String] = spark.read.textFile("D:\\projectdata\\app_dict.txt").rdd
  dicRDD.map(_.split("\t",-1)).filter(_.length>5).foreachPartition(arr=>{
    val jedis: Jedis = JedisConnectionPool.getConnection()
    arr.foreach(arr=>{
      jedis.set(arr(4),arr(1))
    })
    jedis.close()
  })
  spark.stop()
}
}
