package day02.dataset;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.dataset
 * @Author: luk
 * @CreateTime: 2020/3/19 16:38
 */
public class FlinkJoinJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> arr1 = new ArrayList<>();
        Tuple2<Integer, String> tuple1 = new Tuple2<>(1, "张三");
        Tuple2<Integer, String> tuple2 = new Tuple2<>(2,"李四");
        Tuple2<Integer, String> tuple3 = new Tuple2<>(3,"王五");
        arr1.add(tuple1);
        arr1.add(tuple2);
        arr1.add(tuple3);

        ArrayList<Tuple2<Integer, String>> arr2 = new ArrayList<>();
        Tuple2<Integer, String> tuple4 = new Tuple2<>(1, "18");
        Tuple2<Integer, String> tuple5 = new Tuple2<>(2,"20");
        Tuple2<Integer, String> tuple6 = new Tuple2<>(4,"25");
        arr2.add(tuple4);
        arr2.add(tuple5);
        arr2.add(tuple6);

        DataSource<Tuple2<Integer, String>> firstDataSetStream = env.fromCollection(arr1);
        DataSource<Tuple2<Integer, String>> secondDataSetStream = env.fromCollection(arr2);

        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> joinDataSet = firstDataSetStream.join(secondDataSetStream)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });

       // joinDataSet.print();

        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> leftOutJoinDataSet = firstDataSetStream.leftOuterJoin(secondDataSetStream)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(second==null) {
                            return new Tuple3<>(first.f0, first.f1, "null");
                        }else {
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }
                    }
                });

        leftOutJoinDataSet.print();

        env.execute();
    }
}
