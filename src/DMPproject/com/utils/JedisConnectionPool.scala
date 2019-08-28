package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
//  val config = new JedisPoolConfig()
//  config.setMaxTotal(20)
//  config.setMaxIdle(10)
//  val pool = new JedisPool(config,"hadoop02",6379)
val pool = new JedisPool("hadoop02",6379)
  def getConnection():Jedis={

    pool.getResource
  }
}
