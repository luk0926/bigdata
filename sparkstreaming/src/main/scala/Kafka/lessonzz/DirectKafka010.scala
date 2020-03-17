//package lessonzz
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010._
//
//object DirectKafka010 {
//  def main(args: Array[String]): Unit = {
//   // Logger.getLogger("org").setLevel(Level.ERROR)
//    //步骤一：获取配置信息
//    val conf = new SparkConf().setAppName("DirectKafka010")
//    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//    val ssc = new StreamingContext(conf,Seconds(5))
//
//    val brokers = "10.148.15.38:9092"
//    val topic = args(0)
//    val topics = topic
//    val groupId = "hdp_ubu_ershou_youpin_h_test" //注意，这个也就是我们的消费者的名字
//
//    val topicsSet = topics.split(",").toSet
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> brokers,
//      "group.id" -> groupId,
//      //sparkstremaing消费的kafka的一条消息，最大可以多大
//      //默认是1M，比如可以设置为10M，生产里面一般都是设置10M。
//      "fetch.message.max.bytes" -> "20971520000",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer]
//    )
//
//    //步骤二：获取数据源
//    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
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
