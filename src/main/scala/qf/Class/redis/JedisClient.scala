package qf.Class.redis

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisClient {

  //获取连接
  def getConnection() ={
    val config = new JedisPoolConfig()
    //最大连接数
    config.setMaxTotal(30)
    //最大空闲连接数
    config.setMaxIdle(10)
    //是否进行有效性检查
    config.setTestOnBorrow(true)
    //config,host,port ,连接超时时长
    val pool = new JedisPool(config,"node1",6379,100000)
    val resource: Jedis = pool.getResource
    resource
  }
  def main(args: Array[String]): Unit = {
    //获取连接
    val jedis = getConnection()
    //字符串类型的操作
    jedis.set("jstrkey","strvalue_jedis")
    val str: String = jedis.get("jstrkey")
    println(str)
    jedis.del("jstrkey")
    jedis.mset("jstrkey1","jstrvalue","jstrkey2","jstrvalue2")
    val list: util.List[String] = jedis.mget("jstrkey","jstrkey1","jstrkey2")
    println(list)
    //list类型
    jedis.lpush("listkey","scala","java","c")
    jedis.rpush("listkey","python")
    jedis.rpop("listkey")
   // jedis.append("listkey","chinese")
    val list1: util.List[String] = jedis.lrange("listkey",0,-1)
    println(list1)
    //资源释放
    jedis.close()

  }

}
