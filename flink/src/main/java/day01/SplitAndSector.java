package day01;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/18 11:38
 */
public class SplitAndSector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add("hello world");
        arrayList.add("spark flink");

        DataStreamSource<String> dataStreamSource = env.fromCollection(arrayList);

        SplitStream<String> splitStream = dataStreamSource.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                ArrayList<String> sectorName = new ArrayList<>();

                if (value.contains("hello")) {
                    sectorName.add("hello");
                } else {
                    sectorName.add("other");
                }

                return sectorName;
            }
        });

        splitStream.shuffle();
        splitStream.rescale();

        splitStream.select("hello").print();

        env.execute();
    }
}
