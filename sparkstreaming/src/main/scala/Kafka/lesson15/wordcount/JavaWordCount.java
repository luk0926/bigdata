package Kafka.lesson15.wordcount;//package lesson15.wordcount;
//
//
//import kafka.serializer.StringDecoder;
//import lesson15.kafkaoffset.KafkaManager;
//import lesson15.kafkaoffset.KaikebaListener;
//import lesson15.kafkaoffset.TypeHelper;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import scala.Tuple2;
//
//import java.util.*;
//
//
//public class JavaWordCount {
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
//        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
//        String topics="test"; //主题
//        String groupId="test_consumer";//你的consumer的名字
//        String brokers="hadoop2:9092";//brokers
//        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
//        Map<String, String> kafkaParams = new HashMap<>();//kafka参数
//
//        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        //关键步骤一：增加监听器，批次完成时自动帮你自动提交偏移量
//        ssc.addStreamingListener(new KaikebaListener(TypeHelper.toScalaImmutableMap(kafkaParams)));
//        //关键步骤二：使用数据平台提供的KafkaManager，根据偏移量获取数据
//        // 如果你是Java代码 调用createDirectStream
//        final KafkaManager kafkaManager = new KafkaManager(TypeHelper.toScalaImmutableMap(kafkaParams));
//        JavaPairInputDStream<String, String> myDStream = kafkaManager.createDirectStream(
//                ssc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                topicsSet
//        );
//
//        myDStream.map(new Function<Tuple2<String,String>, String>() {
//            @Override
//            public String call(Tuple2<String, String> tuple) throws Exception {
//                return tuple._2;
//            }
//        }).flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList(line.split("_")).iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word,1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer a, Integer b) throws Exception {
//                return a+b;
//            }
//        }).foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
//            @Override
//            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
//              rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//                  @Override
//                  public void call(Tuple2<String, Integer> wordCount) throws Exception {
//                      System.out.println("单词："+ wordCount._1 + "  "+ "次数："+wordCount._2);
//                  }
//              });
//            }
//        });
//
//        ssc.start();
//        try {
//            ssc.awaitTermination();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        ssc.stop();
//    }
//
//
//}
