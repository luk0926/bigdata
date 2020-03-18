package day01;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/18 11:19
 */
public class ConnectStreamJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> arr1 = new ArrayList<>();
        arr1.add("spark");
        arr1.add("flink");

        ArrayList<Integer> arr2 = new ArrayList<>();
        arr2.add(5);
        arr2.add(10);

        DataStreamSource<String> firstStream = env.fromCollection(arr1);
        DataStreamSource<Integer> secondStream = env.fromCollection(arr2);

        ConnectedStreams<String, Integer> connect = firstStream.connect(secondStream);

        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + ";";
            }

            @Override
            public String map2(Integer value) throws Exception {
                return String.valueOf(value * 2);
            }
        });

        map.print();


        env.execute();
    }
}
