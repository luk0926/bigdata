package Kafka.lesson16

import Kafka.lesson16.offset.KaikebaListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafka010Kafka {
  def main(args: Array[String]): Unit = {
   // Logger.getLogger("org").setLevel(Level.ERROR)
    //步骤一：获取配置信息
    val conf = new SparkConf().setAppName("DirectKafka010").setMaster("local[5]")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    val ssc = new StreamingContext(conf,Seconds(5))

    val brokers = "192.168.167.254:9092"
    val topics = "flink"
    val groupId = "flink_consumer2" //注意，这个也就是我们的消费者的名字

    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupId,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //步骤二：获取数据源
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //设置监听器
    ssc.addStreamingListener(new KaikebaListener(stream))

    val result = stream.map(_.value()).flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)


    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
