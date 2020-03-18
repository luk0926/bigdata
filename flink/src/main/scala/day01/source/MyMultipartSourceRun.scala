package day01.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.source
 * @Author: luk
 * @CreateTime: 2020/3/18 10:13
 */
object MyMultipartSourceRun {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val source: DataStream[Long] = env.addSource(new MultipartSource).setParallelism(1)

    val fileterStream: DataStream[Long] = source.filter(_ % 2 == 0)
    val resultStream: DataStream[AnyVal] = fileterStream.map(t => {
      if (t == 10) {
        new MultipartSource().cancel()
      } else {
        t
      }
    })

    resultStream.print()

    env.execute()
  }
}
