package day03.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day03.kafka
 * @Author: luk
 * @CreateTime: 2020/3/23 18:07
 */
object FlinkKafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._


    //checkpoint配置//checkpoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置statebackend
    env.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink_kafka_sink/checkpoints", true))

    val dataStream: DataStream[String] = env.socketTextStream("node03",9999)

    val topic = "test"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","node01:9092")
    prop.setProperty("group.id","kafka_group1")

    //第一种解决方案，设置FlinkKafkaProducer里面的事务超时时间
    //设置事务超时时间
    prop.setProperty("transaction.timeout.ms",60000*15+"");

    val kafkaSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    dataStream.addSink(kafkaSink)

    env.execute()
  }
}
