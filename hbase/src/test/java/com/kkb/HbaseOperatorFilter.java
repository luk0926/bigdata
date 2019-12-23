package com.kkb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kkb
 * @Author: luk
 * @CreateTime: 2019/12/23 17:43
 */
public class HbaseOperatorFilter {
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
     * 查询所有的rowkey比0003小的所有的数据
     */
    @Test
    public void rowFilter() throws IOException {
        Scan scan = new Scan();
        //获取我们比较对象
        BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());
        /***
         * rowFilter需要加上两个参数
         * 第一个参数就是我们的比较规则
         * 第二个参数就是我们的比较对象
         */
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);
        //为我们的scan对象设置过滤器
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);

                //判断id和age字段，这两个字段是整形值
                if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toInt(value));
                } else {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
    }

    /**
     * 通过familyFilter来实现列族的过滤
     * 需要过滤，列族名包含f2
     * f1  f2   hello   world
     */
    @Test
    public void familyFilter() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("f2");
        //通过familyfilter来设置列族的过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);

        printlReult(scanner);
    }

    /**
     * 列名过滤器 只查询包含name列的值
     */
    @Test
    public void qualifierFilter() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("name");
        //定义列名过滤器，只查询列名包含name的列
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 查询哪些字段值  包含数字8
     */
    @Test
    public void contains8() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("8");
        //列值过滤器，过滤列值当中包含数字8的所有的列
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * select  *  from  myuser where name  = '刘备'
     * 会返回我们符合条件数据的所有的字段
     * <p>
     * SingleColumnValueExcludeFilter  列值排除过滤器
     * select  *  from  myuser where name  ！= '刘备'
     */
    @Test
    public void singleColumnValueFilter() throws IOException {
        //查询 f1  列族 name  列  值为刘备的数据
        Scan scan = new Scan();
        //单列值过滤器，过滤  f1 列族  name  列  值为刘备的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }


    /**
     * 查询rowkey前缀以  00开头的所有的数据
     */
    @Test
    public void prefixFilter() throws IOException {
        Scan scan = new Scan();
        //过滤rowkey以  00开头的数据
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * HBase当中的分页
     */
    @Test
    public void hbasePageFilter() throws IOException {
        int pageNum = 3;
        int pageSize = 2;
        Scan scan = new Scan();
        if (pageNum == 1) {
            //获取第一页的数据
            scan.setMaxResultSize(pageSize);
            scan.setStartRow("".getBytes());
            //使用分页过滤器来实现数据的分页
            PageFilter filter = new PageFilter(pageSize);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            printlReult(scanner);
        } else {
            String startRow = "";
            //扫描数据的调试 扫描五条数据
            int scanDatas = (pageNum - 1) * pageSize + 1;
            scan.setMaxResultSize(scanDatas);//设置一步往前扫描多少条数据
            PageFilter filter = new PageFilter(scanDatas);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] row = result.getRow();//获取rowkey
                //最后一次startRow的值就是0005
                startRow= Bytes.toString(row);//循环遍历我们多有获取到的数据的rowkey
                //最后一条数据的rowkey就是我们需要的起始的rowkey
            }

            //获取第三页的数据
            scan.setStartRow(startRow.getBytes());
            scan.setMaxResultSize(pageSize);//设置我们扫描多少条数据
            PageFilter filter1 = new PageFilter(pageSize);
            scan.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan);
            printlReult(scanner1);
        }

    }

    /**
     * 查询  f1 列族  name  为刘备数据值
     * 并且rowkey 前缀以  00开头数据
     */
    @Test
    public void filterList() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }


    private void printlReult(ResultScanner scanner) {
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);

                //判断id和age字段，这两个字段是整形值
                if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toInt(value));
                } else {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
    }
}
