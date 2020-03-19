//package Kafka.lesson14
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010._
//
//object DirectKafka010 {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    //步骤一：获取配置信息
//    val conf = new SparkConf().setAppName("DirectKafka010").setMaster("local[5]")
//    conf.set("spark.streaming.kafka.maxRatePerPartition", "50")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//    val ssc = new StreamingContext(conf,Seconds(5))
//    val groupId = "flink_consumer" //注意，这个也就是我们的消费者的名字
//
//    val topics = "flink"
//
//    val topcsSet = topics.split(",").toSet
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "192.168.167.254:9092",
//      "group.id" -> groupId,
//      //sparkstremaing消费的kafka的一条消息，最大可以多大
//      //默认是1M，比如可以设置为10M，生产里面一般都是设置10M。
//      "fetch.message.max.bytes" -> "209715200",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer]
//    )
//
//    //步骤二：获取数据源
//    val stream= KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topcsSet, kafkaParams))
//
//    val result = stream.map(_.value()).flatMap(_.split(","))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//
//    result.print()
//
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//
//  }
//
//}
