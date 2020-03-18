package day01.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.source
 * @Author: luk
 * @CreateTime: 2020/3/18 10:00
 */
object MySourceRun {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val source: DataStream[Long] = env.addSource(new MySource).setParallelism(1)

    source.filter(_ % 2 == 0).print()

    env.execute()
  }
}
