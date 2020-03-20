package day02.hbase

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.hbase
 * @Author: luk
 * @CreateTime: 2020/3/20 10:34
 */
object FlinkWriteHBase {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val sourceDataSet: DataSet[String] = env.fromElements("01,zhangsan,28","02,lisi,30")
    sourceDataSet.output(new HBaseOutputFormat)

    env.execute()
  }
}
