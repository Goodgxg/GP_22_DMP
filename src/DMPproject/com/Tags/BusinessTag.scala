package com.Tags

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtil, JedisConnectionPool, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * 商圈标签.
  */
object BusinessTag extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取广告为类型

    var long = 0.0
    var lat = 0.0
    if (StringUtils.isNotBlank(row.getAs[String]("long_fix")) || StringUtils.isNotBlank(row.getAs[String]("lat"))) {
      long = row.getAs[String]("long_fix").toDouble
      lat = row.getAs[String]("lat").toDouble
    }
    //获取经纬度.过滤经纬度
    if (long >= 72 && long <= 136
      && lat >= 2 && lat < 55) {
      //通过经纬度获取商圈
      val business = getBusiness(long, lat)
      if (StringUtils.isNotBlank(business)) {
        val lines = business.split(",")
        lines.foreach(f => list :+= (f, 1))
      }
    }
    list
  }

  /**
    * 获取商圈信息
    * 从Redis中查取,没有,从高德获取.
    */
  def getBusiness(long: Double, lat: Double): String = {
    //转换geohash
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    var busniess: String = redis_queryBusiness(geoHash)
    if (!StringUtils.isNotBlank(busniess) || busniess.length == 0) {
      busniess = AmapUtil.getBusinessFromAmap(long, lat)
      redis_insertBusiness(geoHash, busniess)
    }
    busniess
  }


  /**
    * 获取商圈信息
    */
  def redis_queryBusiness(geoHash: String): String = {
    val jedis: Jedis = JedisConnectionPool.getConnection()
    val busniess = jedis.get(geoHash)
    jedis.close()
    busniess
  }

  /**
    * 存储商圈到Redis
    */
  def redis_insertBusiness(geoHash: String, business: String): Unit = {
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geoHash, business)
    jedis.close()
  }
}
