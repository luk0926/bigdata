package tableapi;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day04.tableapi
 * @Author: luk
 * @CreateTime: 2020/3/25 14:29
 */
public class TableCSV {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(localEnvironment);

        //  String path = JavaStreamWordCount.class.getClassLoader().getResource("words.txt").getPath();
        CsvTableSource csvSource = CsvTableSource.builder().field("id", Types.INT())
                .field("name", Types.STRING())
                .field("age", Types.INT())
                .fieldDelimiter(",")
                .ignoreParseErrors()
                .ignoreFirstLine()
                .path("D:\\资料\\开课吧\\20_flink进阶\\4、flink-第四天——课前资料\\数据\\flinksql.csv")
                .build();
        tEnv.registerTableSource("myUser",csvSource);

        Table sqlTable = tEnv.sqlQuery("select * from myUser  where age > 20");

        tEnv.toRetractStream(sqlTable, Row.class).print();

        tEnv.execute("csvTable");
    }
}
