package com.vita.redies

import com.vita.Constants
import org.apache.log4j.{Level, Logger}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class RedisSingle {
  var ip: String = null
  var port: Int = 0
  var auth: String = null
  var pool: JedisPool = null

  val SUCCESS_OK = "OK"
  val SUCCESS_STATUS_LONG = 1L

  def init(ip: String, port: Int): Unit = {
    this.ip = ip
    this.port = port

    val config: JedisPoolConfig = new JedisPoolConfig
    //最大空间连接数，默认8个
    config.setMaxTotal(8)
    //最大连接数，默认8个
    config.setMaxIdle(8)
    //timeout：连接 redis 时候的超时
    // JedisPool(jedisPoolConfig,redisHost,redisPort,timeout,redisPassword)
    pool = new JedisPool(config, ip, port, 3000)
    //var pool = new JedisPool(config,ip,part,3000)
  }

  def getJedis(): Jedis = {
    var rds: Jedis = null
    try {
      rds = pool.getResource()
    } catch {
      case e: Exception => {
        println("jedis error:E " + e)
      }
    }
    rds
    /* finally {
       if (rds != null) {
         // pool.returnResource(rds)
         /*
           在 jedis2.8.2 版本中已将 JedisPool#returnResource 方法废弃,
           并明确说明这个方法的功能由Jedis.close() 方法代替
         */
         rds.close()
       }
     }
     pool.close()*/
  }

  def getPool(): JedisPool = {
    pool
  }

  /**
    * 在finaally中回收jedis
    */
  def returnResource(jedis: Jedis): Unit = {
    if (jedis != null && pool != null) {
      pool.returnResource(jedis)
    }
  }

  /**
    * 设置key的过期时间，以秒为单位
    *
    * @param key
    * @param seconds
    */
  def expire(key: String, seconds: Int): Long = {
    if (seconds < 0) {
      return -1
    }

    val jedis: Jedis = getJedis()
    val count: Long = jedis.expire(key, seconds)
    count
  }

  def flushAll(): Unit = {
    val jedis: Jedis = getJedis()
    if (jedis != null) {
      val stats: String = jedis.flushAll()
      returnResource(jedis)
    }
  }

  /**
    *
    * @param key
    * @param value
    * @param nxxx NX|XX  是否存在
    *             <li>NX -- Only set the key if it does not already exist.</li>
    *             <li>XX -- Only set the key if it already exist.</li>
    * @param expx EX|PX, expire time units ，时间单位格式，秒或毫秒
    *             <li>EX = seconds;</li>
    *             <li>PX = milliseconds</li>
    * @param time expire time in the units of expx，时间（long型），不能小于0
    * @return
    */
  def set(key: String, value: String, nxxx: String, expx: String, time: Long): Boolean = {
    if (time < 0)
      false

    val jedis: Jedis = getJedis()
    val statusCode: String = jedis.set(key, value, nxxx, expx, time)
    if (statusCode.equalsIgnoreCase(SUCCESS_OK)) {
      true
    }
    false
  }

  def set(key: String, value: String, nxxx: String, expx: String): Unit = {
    set(key, value, nxxx, "EX", 60 * 60)
  }

  /**
    * 成功返回true
    *
    * @param key
    * @param value
    * @return
    */
  def set(key: String, value: String): Boolean = {
    val jedis: Jedis = getJedis()
    if (jedis != null) {
      val statusCode: String = jedis.set(key, value)
      if (statusCode.equalsIgnoreCase(SUCCESS_OK)) {
        return true
      }
    }
    false
  }

  /**
    * 返回值
    *
    * @param key
    * @return
    */
  def get(key: String): String = {
    val jedis: Jedis = getJedis()
    jedis.get(key)
  }

  /**
    * 判断key是否存在，存在返回true
    *
    * @param key
    * @return
    */
  def exists(key: String): Boolean = {
    val jedis: Jedis = getJedis()
    jedis.exists(key)
  }

  /**
    * 判断key是否存在，存在返回true
    *
    * @param key
    * @return
    */
  def isExists(key: String): Boolean = {
    exists(key)
  }

  /**
    * 获取剩余时间（秒）
    *
    * @param key
    * @return
    */
  def getTime(key: String): Long = {
    val jedis: Jedis = getJedis()
    try {
      return jedis.ttl(key)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    -1L
  }

  /**
    * 设置超期时间
    *
    * @param key
    * @param seconds 为Null时，将会马上过期。可以设置-1，0，表示马上过期
    * @return
    */
  def setTime(key: String, seconds: Int): Boolean = {
    val jedis = getJedis()
    var sec = seconds
    try {
      if (seconds <= 0) {
        sec = seconds
      }
      val statusCode: Long = jedis.expire(key, sec)
      if (SUCCESS_STATUS_LONG == statusCode) {
        true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    false
  }

  /**
    * 删除key及值
    *
    * @param key
    * @return
    */
  def del(key: String): Boolean = {
    val jedis: Jedis = getJedis()
    val statusCode: Long = jedis.del(key)
    if (statusCode == SUCCESS_STATUS_LONG) {
      true
    }
    false
  }

  /**
    * 设置key值和过期时间
    *
    * @param key
    * @param value
    * @param seconds 秒数，不能小于0
    * @return
    */
  def setByTime(key: String, value: String, seconds: Int): Boolean = {
    if (seconds < 0)
      false

    val jedis: Jedis = getJedis()
    val statusCode: String = jedis.setex(key, seconds, value)
    if (statusCode.equalsIgnoreCase(SUCCESS_OK)) {
      true
    }
    false
  }

}

object RedisSingle {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val redisSingle: RedisSingle = new RedisSingle()
    redisSingle.init("127.0.0.1", 6379)
    //    println(redisSingle.get(Constants.REDIDS_KEY))
    if (!redisSingle.exists(Constants.REDIDS_KEY)) {
      redisSingle.set(Constants.REDIDS_KEY, "value")
      redisSingle.setTime(Constants.REDIDS_KEY, 60 * 60)
    } else {
      println(redisSingle.getTime(Constants.REDIDS_KEY))
      //      redisSingle.del(Constants.REDIDS_KEY)
    }

    //    redisSingle.flushAll()
  }
}
