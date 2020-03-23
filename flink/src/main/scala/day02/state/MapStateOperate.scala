package day02.state

import java.util.UUID

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.state
 * @Author: luk
 * @CreateTime: 2020/3/23 12:11
 */
object MapStateOperate {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (2L, 4d),
      (2L, 2d),
      (2L, 6d)
    )).keyBy(_._1)
      .flatMap(new CountWithAverageMapState)
      .print()
    env.execute()
  }
}



class CountWithAverageMapState extends RichFlatMapFunction[(Long,Double),(Long,Double)]{
  private var mapState:MapState[String,Double] = _
  //初始化获取mapState对象
  override def open(parameters: Configuration): Unit = {
    val mapStateOperate = new MapStateDescriptor[String,Double]("mapStateOperate",classOf[String],classOf[Double])
    mapState = getRuntimeContext.getMapState(mapStateOperate)
  }
  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    //将相同的key对应的数据放到一个map集合当中去，就是这种对应  1 -> List[Map,Map,Map]
    //每次都构建一个map集合
    mapState.put(UUID.randomUUID().toString,input._2)
    import scala.collection.JavaConverters._
    //获取map集合当中所有的value，我们每次将数据的value给放到map的value里面去
    val listState: List[Double] = mapState.values().iterator().asScala.toList
    if(listState.size >=3){
      var count = 0L
      var sum = 0d
      for(eachState <- listState){
        count +=1
        sum += eachState
      }
      println("average"+ sum/count)
      out.collect(input._1,sum/count)
    }
  }
}