package com.utils


import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}



object JedisUtils {
  private val config = new JedisPoolConfig()

  // 最大连接数
  config.setMaxTotal(20)
  // 最大空闲连接数
  config.setMaxIdle(10)
  val properties = new Properties()
  properties.load(this.getClass.getClassLoader.getResourceAsStream("jedis.properties"))
  val host=properties.getProperty("host")
  val port=properties.getProperty("port").toInt
  val password=properties.getProperty("password")
  // 加载config
  private val pool = new JedisPool(config,host,port,10000,password)

  //获取连接
  def getConnection():Jedis={
    pool.getResource
  }



}
