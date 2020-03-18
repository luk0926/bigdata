package day01.sink

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.sink
 * @Author: luk
 * @CreateTime: 2020/3/18 15:08
 */
class MyRedisMapper extends RedisMapper[Tuple2[String, String]]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(t: (String, String)): String = {
    t._1
  }

  override def getValueFromData(t: (String, String)): String = {
    t._2
  }
}
