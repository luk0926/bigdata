package day02.state.liststate

import java.lang
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.state.liststate
 * @Author: luk
 * @CreateTime: 2020/3/23 11:48
 */
class CountWindowAverageWithList extends RichFlatMapFunction[(Long,Double),(Long,Double)]{

  //定义我们历史所有的数据获取
  private var elementsByKey:ListState[(Long,Double)] = _


  override def open(parameters: Configuration): Unit = {
    //初始化获取历史状态的值，每个key对应的所有历史值，都存储在list集合里面了
    val listState = new ListStateDescriptor[(Long,Double)]("listState",classOf[(Long,Double)])
    elementsByKey = getRuntimeContext.getListState(listState)

  }

  override def flatMap(element: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    val currentState: lang.Iterable[(Long, Double)] = elementsByKey.get()//获取当前key的状态值
    //如果初始状态为空，那么就进行初始化，构造一个空的集合出来，准备 用于存储后续的数据
    if(currentState ==null){
      elementsByKey.addAll(Collections.emptyList())
    }
    elementsByKey.add(element)
    import scala.collection.JavaConverters._
    val allElements: Iterator[(Long, Double)] = elementsByKey.get().iterator().asScala
    val allElementList: List[(Long, Double)] = allElements.toList
    if(allElementList.size >= 3){
      var count = 0L
      var sum = 0d
      for(eachElement <- allElementList){
        count +=1
        sum += eachElement._2
      }
      out.collect((element._1,sum/count))
    }
  }
}
