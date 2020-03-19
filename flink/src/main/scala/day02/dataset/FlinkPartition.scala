package day02.dataset

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.dataset
 * @Author: luk
 * @CreateTime: 2020/3/19 16:17
 */
object FlinkPartition {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val array1 = new ArrayBuffer[Tuple2[Int,String]]()
    array1.+=(new Tuple2(1,"hello1") )
    array1.+=(new Tuple2(2,"hello2") )
    array1.+=(new Tuple2(2,"hello3") )
    array1.+=(new Tuple2(3,"hello4") )
    array1.+=(new Tuple2(3,"hello5") )
    array1.+=(new Tuple2(3,"hello6") )
    array1.+=(new Tuple2(4,"hello7") )
    array1.+=(new Tuple2(4,"hello8") )
    array1.+=(new Tuple2(4,"hello9") )
    array1.+=(new Tuple2(4,"hello10"))
    array1.+=(new Tuple2(5,"hello11"))
    array1.+=(new Tuple2(5,"hello12"))
    array1.+=(new Tuple2(5,"hello13"))
    array1.+=(new Tuple2(5,"hello14"))
    array1.+=(new Tuple2(5,"hello15"))
    array1.+=(new Tuple2(6,"hello16"))
    array1.+=(new Tuple2(6,"hello17"))
    array1.+=(new Tuple2(6,"hello18"))
    array1.+=(new Tuple2(6,"hello19"))
    array1.+=(new Tuple2(6,"hello20"))
    array1.+=(new Tuple2(6,"hello21"))

    val dataSetStream: DataSet[(Int, String)] = env.fromCollection(array1)

    // HashPartition
//    val hashPartition: DataSet[(Int, String)] = dataSetStream.partitionByHash(0).mapPartition(eachPartition => {
//      while (eachPartition.hasNext) {
//        val tuple: (Int, String) = eachPartition.next()
//
//        println("当前线程id：" + Thread.currentThread().getId + "---" + tuple._1)
//      }
//      eachPartition
//    })
//
//    hashPartition.print()

    // RangePartition
    val rangePartition: DataSet[(Int, String)] = dataSetStream.partitionByRange(_._1).mapPartition(eachPartition => {
      while (eachPartition.hasNext) {
        val tuple: (Int, String) = eachPartition.next()

        println("当前线程ID为" + Thread.currentThread().getId + "=============" + tuple._1)
      }
      eachPartition
    })

    rangePartition.print()

    env.execute()
  }
}
