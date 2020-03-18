package day01.partition

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.partition
 * @Author: luk
 * @CreateTime: 2020/3/18 14:04
 */
object FlinkCustomerPartition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.setParallelism(2)

    val sourceStream: DataStream[String] = env.fromElements("hello word", "hello spark", "hive hadoop")

    val repartition: DataStream[String] = sourceStream.partitionCustom(new MyPartitioner, x => x)
    repartition.map(x => {
      println("数据的key为：" + x, "线程为：" + Thread.currentThread().getId)

      x
    })

    repartition.print()

    env.execute()
  }
}
