package day02.dataset

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.dataset
 * @Author: luk
 * @CreateTime: 2020/3/19 14:53
 */
object FlinkDataSet {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val arrayBuffer = new ArrayBuffer[String]()
    arrayBuffer.+=("hello world1")
    arrayBuffer.+=("hello world2")
    arrayBuffer.+=("hello world3")
    arrayBuffer.+=("hello world4")

    val dataSetStream: DataSet[String] = env.fromCollection(arrayBuffer)

    val resultPartition: DataSet[String] = dataSetStream.mapPartition(eachPartition => {

      eachPartition.map(eachLine => {
        val returnValue = eachLine + ";"

        returnValue
      })
    })

    resultPartition.print()

    env.execute()
  }
}
