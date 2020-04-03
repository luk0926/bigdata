package watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: watermark
 * @Author: luk
 * @CreateTime: 2020/4/2 16:06
 * <p>
 * 定义一个窗口为10s，
 * 通过数据的event time时间结合watermark实现延迟10s的数据也能够正确统计
 */
public class FlinkWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("node03", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> tupleStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(" ");

                return new Tuple2<>(split[0], Long.parseLong(split[1]));
            }
        });

        tupleStream.assignTimestampsAndWatermarks(new MyWatermarks())
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .apply(new MyWindowFunction())
                .print();

        env.execute("FlinkWaterMark");
    }
}


class MyWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

    private Long currentTimemillis = 0L;
    private Long maxWaterMarks = 10000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimemillis - maxWaterMarks);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
        Long enventTime = element.f1;
        currentTimemillis = Math.max(enventTime, currentTimemillis);

        return enventTime;
    }
}

class MyWindowFunction implements WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow> {

    /**
     *
     * @param key      输入数据类型
     * @param window   窗口
     * @param input     窗口输入数据
     * @param out       输出数据
     * @throws Exception
     */
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
        String keyStr = key.toString();
        ArrayList<Long> list = new ArrayList<>();
        Iterator<Tuple2<String, Long>> iterator = input.iterator();
        while (iterator.hasNext()) {
            Tuple2<String, Long> tup = iterator.next();
            list.add(tup.f1);
        }

        list.sort(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return (int) (o1 - o2);
            }
        });

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        String result = "聚合数据的key为：" + keyStr + "," + "窗口当中数据的条数为：" + list.size()
                + "," + "窗口当中第一条数据为：" + "窗口起始时间为：" + sdf.format(window.getStart()) + ","
                + "窗口结束时间为：" + sdf.format(window.getEnd()) + ","
                + "窗口当中第一条数据为：" + sdf.format(list.get(0)) + "," + "窗口当中最后一条数据为：" + sdf.format(list.get(list.size() - 1));

        out.collect(result);
    }
}