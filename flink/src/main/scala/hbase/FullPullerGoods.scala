package hbase

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.flink.api.scala.{ ExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job


/**
 * @BelongsProject: bigdata
 * @BelongsPackage: hbase
 * @Author: luk
 * @CreateTime: 2020/4/9 17:27
 */
object FullPullerGoods {
  //全量拉取商品表数据到HBase里面来
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val inputJdbc: JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://node03:3306/product?characterEncodint=utf-8")
      .setPassword("123456")
      .setUsername("root")
      .setFetchSize(2)
      .setQuery("select * from kaikeba_goods")
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .finish()

    //读取jdbc里面的数据
    val goodsSet: DataSet[Row] = environment.createInput(inputJdbc)

    val result: DataSet[(Text, Mutation)] = goodsSet.map(x => {
      val goodsId: String = x.getField(0).toString
      val goodsName: String = x.getField(1).toString
      val sellingPrice: String = x.getField(2).toString
      val productPic: String = x.getField(3).toString
      val proudctBrand: String = x.getField(4).toString
      val proudctfbl: String = x.getField(5).toString
      val productNum: String = x.getField(6).toString
      val productUrl: String = x.getField(7).toString
      val productFrom: String = x.getField(8).toString
      val goodsStock: String = x.getField(9).toString
      val appraiseNum: String = x.getField(10).toString
      val rowkey = new Text(goodsId)
      val put = new Put(rowkey.getBytes)
      put.addColumn("f1".getBytes(), "goodsName".getBytes(), goodsName.getBytes())
      put.addColumn("f1".getBytes(), "sellingPrice".getBytes(), sellingPrice.getBytes())
      put.addColumn("f1".getBytes(), "productPic".getBytes(), productPic.getBytes())
      put.addColumn("f1".getBytes(), "proudctBrand".getBytes(), proudctBrand.getBytes())
      put.addColumn("f1".getBytes(), "proudctfbl".getBytes(), proudctfbl.getBytes())
      put.addColumn("f1".getBytes(), "productNum".getBytes(), productNum.getBytes())
      put.addColumn("f1".getBytes(), "productUrl".getBytes(), productUrl.getBytes())
      put.addColumn("f1".getBytes(), "productFrom".getBytes(), productFrom.getBytes())
      put.addColumn("f1".getBytes(), "goodsStock".getBytes(), goodsStock.getBytes())
      put.addColumn("f1".getBytes(), "appraiseNum".getBytes(), appraiseNum.getBytes())
      (rowkey, put.asInstanceOf[Mutation])

    })
    //将数据写入到hbase
    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "node01,node02,node03")
    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    configuration.set(TableOutputFormat.OUTPUT_TABLE,"flink:data_goods")
    //mapreduce.output.fileoutputformat.outputdir
    configuration.set("mapred.output.dir","/tmp2")

    val job: Job = Job.getInstance(configuration)
    result.output(new HadoopOutputFormat[Text,Mutation](new TableOutputFormat[Text],job))
    environment.execute("FullPullerGoods")
  }
}

