package day01;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/18 10:56
 */
public class UnionStreamJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> arr1 = new ArrayList<>();
        arr1.add("spark");
        arr1.add("flink");

        ArrayList<String> arr2 = new ArrayList<>();
        arr2.add("hadoop");
        arr2.add("hive");

        DataStreamSource<String> firstStream = env.fromCollection(arr1);
        DataStreamSource<String> secondStream = env.fromCollection(arr2);

        DataStream<String> union = firstStream.union(secondStream);

        union.print();

        env.execute();
    }
}
