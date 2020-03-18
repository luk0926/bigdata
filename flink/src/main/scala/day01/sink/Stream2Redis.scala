package day01.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.sink
 * @Author: luk
 * @CreateTime: 2020/3/18 15:11
 */
object Stream2Redis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.setParallelism(3)

    val streamSource: DataStream[String] = env.fromElements("hello world","key value")

    val tupleData: DataStream[(String, String)] = streamSource.map(x=>(x.split(" ")(0), x.split(" ")(1)))

    val builder: FlinkJedisPoolConfig.Builder = new FlinkJedisPoolConfig.Builder

    builder.setHost("node01")
    builder.setPort(6379)

    builder.setTimeout(5000)
    builder.setMaxTotal(50)
    builder.setMaxIdle(10)
    builder.setMinIdle(5)

    val config: FlinkJedisPoolConfig = builder.build()

    val redisSink: RedisSink[(String, String)] = new RedisSink[Tuple2[String, String]](config, new MyRedisMapper)

    tupleData.addSink(redisSink)

    env.execute("redisSink")
  }
}
