package day02.dataset

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.dataset
 * @Author: luk
 * @CreateTime: 2020/3/19 15:21
 */
object OuterJoin {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val array1 = new ArrayBuffer[Tuple2[Int, String]]()
    val tuple1 = new Tuple2[Int, String](1, "张三")
    val tuple2 = new Tuple2[Int, String](2, "李四")
    val tuple3 = new Tuple2[Int, String](3, "王五")

    array1.+=(tuple1)
    array1.+=(tuple2)
    array1.+=(tuple3)

    val array2 = new ArrayBuffer[Tuple2[Int, String]]()
    val tuple4 = new Tuple2[Int, String](1, "18")
    val tuple5 = new Tuple2[Int, String](2, "35")
    val tuple6 = new Tuple2[Int, String](4, "42")

    array2.+=(tuple4)
    array2.+=(tuple5)
    array2.+=(tuple6)

    val firstDataStream: DataSet[(Int, String)] = env.fromCollection(array1).setParallelism(1)
    val secondDataStream: DataSet[(Int, String)] = env.fromCollection(array2).setParallelism(1)

    val leftOuterJoinDataStream: JoinFunctionAssigner[(Int, String), (Int, String)] = firstDataStream.leftOuterJoin(secondDataStream)
      .where(0)
      .equalTo(0)

    //左外连接
    val resultDataStream: DataSet[(Int, String, String)] = leftOuterJoinDataStream.apply(new JoinFunction[(Int, String), (Int, String), Tuple3[Int, String, String]] {
      override def join(in1: (Int, String), in2: (Int, String)): (Int, String, String) = {
        val result: (Int, String, String) = if (in2 == null) {
          Tuple3(in1._1, in1._2, "null")
        } else {
          Tuple3(in1._1, in1._2, in2._2)
        }
        result
      }
    })
    //resultDataStream.print()

    //右外连接
    val resultDataStream2: DataSet[(Int, String, String)] = firstDataStream.rightOuterJoin(secondDataStream)
      .where(0)
      .equalTo(0)
      .apply(new JoinFunction[(Int, String), (Int, String), Tuple3[Int, String, String]] {
        override def join(in1: (Int, String), in2: (Int, String)): (Int, String, String) = {
          val result: (Int, String, String) = if (in1 == null) {
            (in2._1, in2._2, "null")
          } else {
            (in2._1, in2._2, in1._2)
          }

          result
        }
      })

    //resultDataStream2.print()

    //全外关联
    val resultDataSet: DataSet[(Int, String, Int, String)] = firstDataStream.fullOuterJoin(secondDataStream)
      .where(0)
      .equalTo(0)
      .apply(new JoinFunction[(Int, String), (Int, String), Tuple4[Int, String, Int, String]] {
        override def join(in1: (Int, String), in2: (Int, String)): (Int, String, Int, String) = {
          val result: (Int, String, Int, String) = if (in1 == null) {
            (0, "null", in2._1, in2._2)
          } else if (in2 == null) {
            (in1._1, in1._2, 0, "null")
          } else {
            (in1._1, in1._2, in2._1, in2._2)
          }
          result
        }
      })

    resultDataSet.print()

    env.execute()
  }
}
