package day03.windows

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day03.windows
 * @Author: luk
 * @CreateTime: 2020/3/23 18:33
 */
object FlinkCountWindowAvg {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val socketStream: DataStream[String] = environment.socketTextStream("node01",9999)
    //统计一个窗口内的数据的平均值
    val socketDatas: DataStreamSink[Double] = socketStream.map(x => (1, x.toInt))
      .keyBy(0)
      //.timeWindow(Time.seconds(10))
      .countWindow(3)
      //通过process方法来统计窗口的平均值
      .process(new MyProcessWindowFunctionclass).print()
    //必须调用execute方法，否则程序不会执行
    environment.execute("count avg")
  }
}

class MyProcessWindowFunctionclass extends ProcessWindowFunction[(Int , Int) , Double , Tuple , GlobalWindow]{
  override def process(key: Tuple, context: Context, elements: Iterable[(Int, Int)], out: Collector[Double]): Unit = {
    var totalNum = 0;
    var countNum = 0;
    for(data <-  elements){
      totalNum +=1
      countNum += data._2
    }
    out.collect(countNum/totalNum)
  }
}
