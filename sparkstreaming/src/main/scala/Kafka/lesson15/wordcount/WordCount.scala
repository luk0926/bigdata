//package lesson15.wordcount
//
//
//import Kafka.lesson15.kafkaoffset.KaikebaListener
//import kafka.serializer.StringDecoder
//import lesson15.kafkaoffset.KafkaManager
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//
//
//object WordCount {
//  def main(args: Array[String]): Unit = {
//    //步骤一：建立程序入口
//    val conf = new SparkConf().setMaster("local[3]").setAppName("wordCount")
//    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//    val ssc = new StreamingContext(conf,Seconds(5))
//
//
//    //步骤二：设置各种参数
//
//    val brokers = "192.168.167.254:9092"
//    val topics = "flink"
//    val groupId = "flink_my_consumer" //注意，这个也就是我们的消费者的名字
//
//    val topicsSet = topics.split(",").toSet
//
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers,
//      "group.id" -> groupId,
//      "enable.auto.commit" -> "false"
//    )
//    //关键步骤一：设置监听器，帮我们完成偏移量的提交
//
//    /**
//      * 监听器的作用就是，我们每次运行完一个批次，就帮我们提交一次偏移量。
//      */
//    ssc.addStreamingListener(
//      new KaikebaListener(kafkaParams));
//
//    //关键步骤二： 创建对象，然后通过这个对象获取到上次的偏移量，然后获取到数据流
//    val km = new KafkaManager(kafkaParams)
//
//    //步骤三：创建一个程序入口
//    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicsSet)
//
//
//
//
//
//     //完成你的业务逻辑即可
//     messages  //只要对messages这个对象做一下操作，里面对偏移量信息就会丢失了
//       .map(_._2)
//       .flatMap(_.split(","))
//       .map((_,1))
//         .foreachRDD( rdd =>{
//           rdd.foreach( line =>{
//             println(line)
//             println("-==============进行业务处理就可以了=====================batch=========")
//             //这儿是没有办法获取到偏移量信息的。
//
//           })
//           println("在这儿提交偏移量")
//         })
//
//
//
//
//
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//
//}
//
