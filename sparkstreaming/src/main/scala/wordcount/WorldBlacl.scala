package wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: wordcount
 * @Author: luk
 * @CreateTime: 2020/3/12 17:03
 */
object WorldBlacl {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    /**
     * 自己模拟一个黑名单：
     * 各位注意：
     * 这个黑名单，一般情况下，不是我们自己模拟出来，应该是从mysql数据库
     * 或者是Reids 数据库，或者是HBase数据库里面读取出来的。
     */
    val wordBlackList: RDD[(String, Boolean)] = sc.parallelize(List("?","!","*")).map((_, true))

    val blackList: Array[(String, Boolean)] = wordBlackList.collect()
    val blackListBrodCast: Broadcast[Array[(String, Boolean)]] = ssc.sparkContext.broadcast(blackList)

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node03", 9999)

    val wordOneDStream: DStream[(String, Int)] = dstream.flatMap(_.split(" ")).map((_, 1))

    //transform 需要有返回值，必须类型是RDD
    val wordCountDstream: DStream[(String, Int)] = wordOneDStream.transform(rdd => {
      val filterRdd: RDD[(String, Boolean)] = rdd.sparkContext.parallelize(blackListBrodCast.value)
      val leftOutJoinRdd: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(filterRdd)

      val resultRdd: RDD[(String, (Int, Option[Boolean]))] = leftOutJoinRdd.filter(_._2._2.isEmpty)
      val value: RDD[(String, Int)] = resultRdd.map(t => (t._1, t._2._1))
      value

    })

    wordCountDstream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
