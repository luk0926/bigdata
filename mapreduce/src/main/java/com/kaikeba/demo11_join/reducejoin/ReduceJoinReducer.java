package com.kaikeba.demo11_join.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo11_join
 * @Author: luk
 * @CreateTime: 2019/12/10 14:29
 */
public class ReduceJoinReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String firsrPart = "";
        String secondPart = "";
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                secondPart = value.toString();
            } else {
                firsrPart = value.toString();
            }
        }

        context.write(new Text(firsrPart + "\t" + secondPart), NullWritable.get());
    }
}
