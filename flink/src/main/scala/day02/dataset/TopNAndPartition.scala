package day02.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.dataset
 * @Author: luk
 * @CreateTime: 2020/3/19 16:10
 */
object TopNAndPartition {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val array1 = new ArrayBuffer[Tuple2[Int,String]]()
    val tuple1 = new Tuple2[Int,String](1,"张三")
    val tuple2 = new Tuple2[Int,String](2,"李四")
    val tuple3 = new Tuple2[Int,String](3,"王五")
    val tuple4 = new Tuple2[Int,String](3,"赵6")
    array1.+=(tuple1)
    array1.+=(tuple2)
    array1.+=(tuple3)
    array1.+=(tuple4)

    val dataSetStream: DataSet[(Int, String)] = env.fromCollection(array1)

    dataSetStream.setParallelism(1).groupBy(0)
        .sortGroup(1, Order.DESCENDING)
        .first(1)
        .print()



    env.execute()
  }
}
