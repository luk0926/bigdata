package day02.hbase

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.hbase
 * @Author: luk
 * @CreateTime: 2020/3/20 10:28
 */
class HBaseOutputFormat extends OutputFormat[String]{
  val zkServer = "node01"
  val port = "2181"
  var conn: Connection = null
  val hbaseTableName = "hbasesource"

  override def configure(configuration: Configuration): Unit = {

  }

  override def open(i: Int, i1: Int): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    config.set(HConstants.ZOOKEEPER_QUORUM, zkServer)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)
  }

  override def writeRecord(it: String): Unit = {
    val tableName: TableName = TableName.valueOf(hbaseTableName)
    val cf1 = "f1"
    val array: Array[String] = it.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    val putList: java.util.ArrayList[Put] = new java.util.ArrayList[Put]
    putList.add(put)
    //设置缓存1m，当达到1m时数据会自动刷到hbase
    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    //设置缓存的大小
    params.writeBufferSize(1024 * 1024)
    val mutator: BufferedMutator = conn.getBufferedMutator(params)
    mutator.mutate(putList)
    mutator.flush()
    putList.clear()
  }

  override def close(): Unit = {
    if(null != conn){
      conn.close()
    }
  }
}
