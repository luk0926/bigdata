package day01.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.source
 * @Author: luk
 * @CreateTime: 2020/3/18 10:05
 */
class MultipartSource extends ParallelSourceFunction[Long]{
  private var number = 1
  private var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      number += 1
      ctx.collect(number)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
