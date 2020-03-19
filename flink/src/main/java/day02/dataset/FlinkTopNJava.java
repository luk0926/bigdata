package day02.dataset;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.dataset
 * @Author: luk
 * @CreateTime: 2020/3/19 16:59
 */
public class FlinkTopNJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> arr = new ArrayList<>();

        Tuple2<Integer, String> tuple1 = new Tuple2<>(1, "张三");
        Tuple2<Integer, String> tuple2 = new Tuple2<>(2, "李四");
        Tuple2<Integer, String> tuple3 = new Tuple2<>(3,"王五");
        Tuple2<Integer, String> tuple4 = new Tuple2<>(3, "赵6");
        arr.add(tuple1);
        arr.add(tuple2);
        arr.add(tuple3);
        arr.add(tuple4);

        DataSource<Tuple2<Integer, String>> dataSet = env.fromCollection(arr);

        dataSet.groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .first(1)
                .print();



        env.execute();
    }
}
