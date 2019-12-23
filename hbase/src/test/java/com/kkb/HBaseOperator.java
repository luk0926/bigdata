package com.kkb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kkb
 * @Author: luk
 * @CreateTime: 2019/12/23 15:37
 */
public class HBaseOperator {

    //操作数据库  第一步：获取连接  第二步：获取客户端对象   第三步：操作数据库  第四步：关闭

    /**
     * 创建一张表  myuser  两个列族  f1   f2
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //连接HBase集群不需要指定HBase主节点的ip地址和端口号
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        //创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取连接对象，创建一张表
        //获取管理员对象，来对手数据库进行DDL的操作
        Admin admin = connection.getAdmin();
        //指定我们的表名
        TableName myuser = TableName.valueOf("myuser");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(myuser);
        //指定两个列族
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }


    private Connection connection;
    private Table table;

    @Before
    public void initTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("myuser"));
    }

    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }


    /**
     * 向myuser表当中添加数据
     */
    @Test
    public void addData() throws IOException {
        //获取表
        Put put = new Put("0001".getBytes());//创建put对象，并指定rowkey值
        put.addColumn("f1".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("地球人"));
        table.put(put);
    }

    /**
     * hbase的批量插入数据
     */
    @Test
    public void batchInsert() throws IOException {
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        //f1
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        //f2
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("talk is cheap , show me the code"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<Put>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        table.put(listPut);
    }

    /**
     * 查询rowkey为0003的人
     */
    @Test
    public void getData() throws IOException {
        //通过get对象，指定rowkey
        Get get = new Get(Bytes.toBytes("0003"));

        get.addFamily("f1".getBytes());//限制只查询f1列族下面所有列的值
        //查询f2  列族 phone  这个字段
        get.addColumn("f2".getBytes(), "phone".getBytes());

        //通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面了
        Result result = table.get(get);
        //获取一条数据所有的cell，所有数据值都是在cell里面 的
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            byte[] family_name = CellUtil.cloneFamily(cell);//获取列族名
            byte[] column_name = CellUtil.cloneQualifier(cell);//获取列名
            byte[] rowkey = CellUtil.cloneRow(cell);//获取rowkey
            byte[] cell_value = CellUtil.cloneValue(cell);//获取cell值

            //需要判断字段的数据类型，使用对应的转换的方法，才能够获取到值
            if ("age".equals(Bytes.toString(column_name)) || "id".equals(Bytes.toString(column_name))) {
                System.out.println(Bytes.toString(family_name));
                System.out.println(Bytes.toString(column_name));
                System.out.println(Bytes.toString(rowkey));
                System.out.println(Bytes.toInt(cell_value));
            } else {
                System.out.println(Bytes.toString(family_name));
                System.out.println(Bytes.toString(column_name));
                System.out.println(Bytes.toString(rowkey));
                System.out.println(Bytes.toString(cell_value));
            }
        }
    }


    /**
     * 不知道rowkey的具体值，我想查询rowkey范围值是0003  到0006
     * select * from myuser  where age > 30  and id < 8  and name like 'zhangsan'
     */
    @Test
    public void scanData() throws IOException {
        //没有指定startRow以及stopRow  全表扫描
        Scan scan = new Scan();

        //只扫描f1列族
        scan.addFamily("f1".getBytes());
        //扫描 f2列族 phone  这个字段
        scan.addColumn("f2".getBytes(), "phone".getBytes());
        scan.setStartRow("0003".getBytes());
        scan.setStopRow("0007".getBytes());
        //通过getScanner查询获取到了表里面所有的数据，是多条数据
        ResultScanner scanner = table.getScanner(scan);
        //遍历ResultScanner 得到每一条数据，每一条数据都是封装在result对象里面了
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] values = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toInt(values));
                } else {
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toString(values));
                }
            }
        }
    }


    /**
     * 删除数据
     */
    @Test
    public  void  deleteData() throws IOException {
        Delete delete = new Delete("0003".getBytes());
        table.delete(delete);
    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() throws IOException {
        //获取管理员对象，用于表的删除
        Admin admin = connection.getAdmin();
        //删除一张表之前，需要先禁用表
        admin.disableTable(TableName.valueOf("myuser"));
        admin.deleteTable(TableName.valueOf("myuser"));
    }

}
