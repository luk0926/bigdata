package day01

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/18 11:04
 */
object ConnectStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val firstStream: DataStream[String] = env.fromCollection(Array("hello world", "spark flink"))

    val secondStream: DataStream[Int] = env.fromCollection(Array(1, 2, 3, 4))

    val connectStream: ConnectedStreams[String, Int] = firstStream.connect(secondStream)

    val resultStream1: DataStream[Any] = connectStream.map(t1 => t1 + ";", t2 => t2 * 2)
    //resultStream1.print()

    val resultStream2: DataStream[String] = connectStream.flatMap(new CoFlatMapFunction[String, Int, String] {
      override def flatMap1(value1: String, out: Collector[String]): Unit = {
        out.collect(value1.toUpperCase())
      }

      override def flatMap2(value2: Int, out: Collector[String]): Unit = {
        out.collect((value2 * 2).toString)
      }
    })

    resultStream2.print()

    env.execute()
  }
}
