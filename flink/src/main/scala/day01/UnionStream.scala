package day01

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/18 10:45
 */
object UnionStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val firstStream: DataStream[String] = env.fromCollection(Array("spark,flink"))
    val secondStream: DataStream[String] = env.fromCollection(Array("hive,hadoop"))

    val resultStream: DataStream[String] = firstStream.union(secondStream)

    resultStream.print()
    

    env.execute();
  }
}
