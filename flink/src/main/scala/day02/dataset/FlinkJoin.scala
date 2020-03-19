package day02.dataset

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.dataset
 * @Author: luk
 * @CreateTime: 2020/3/19 15:10
 */
object FlinkJoin {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val array1 = new ArrayBuffer[Tuple2[Int,String]]()
    val tuple1 = new Tuple2[Int,String](1,"张三")
    val tuple2 = new Tuple2[Int,String](2,"李四")
    val tuple3 = new Tuple2[Int,String](3,"王五")

    array1.+=(tuple1)
    array1.+=(tuple2)
    array1.+=(tuple3)


    val array2 = new ArrayBuffer[Tuple2[String,Int]]()
    val tuple4 = new Tuple2[String, Int]("18", 1)
    val tuple5 = new Tuple2[String,Int]("35",2)
    val tuple6 = new Tuple2[String,Int]("42",3)
    array2.+=(tuple4)
    array2.+=(tuple5)
    array2.+=(tuple6)

    val firstDataStream: DataSet[(Int, String)] = env.fromCollection(array1)
    val secondDataStream: DataSet[(String,Int)] = env.fromCollection(array2)

    val joinResult: UnfinishedJoinOperation[(Int, String), (String,Int)] = firstDataStream.join(secondDataStream)

    val resultDaataSet: JoinDataSet[(Int, String), (String,Int)] = joinResult.where(0).equalTo(1)

    resultDaataSet.print()

    env.execute()
  }
}
