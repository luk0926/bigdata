package wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage:
 * @Author: luk
 * @CreateTime: 2020/3/12 14:27
 */
object MapWithStateAPITest {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint("/tmp/checkpointDir")

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node03", 9999)
    val wordCount: DStream[(String, Int)] = dstream.flatMap(_.split(" "))
      .map((_, 1))

    val initialRDD: RDD[(String, Long)] = sc.parallelize(List(("dummy", 100L), ("source", 32L)))

    val stateSpec = StateSpec.function((currentBatchTime: Time, key: String, value: Option[Int], currentState: State[Long]) => {
      val sum: Long = value.getOrElse(0).toLong + currentState.getOption().getOrElse(0l)
      val outPut: (String, Long) = (key, sum)
      if (!currentState.isTimingOut()) {
        currentState.update(sum)
      }
      Some(outPut)
    }).initialState(initialRDD).timeout(Seconds(10))

    val result: MapWithStateDStream[String, Int, Long, (String, Long)] = wordCount.mapWithState(stateSpec)

    //result.print()

    result.stateSnapshots().print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
