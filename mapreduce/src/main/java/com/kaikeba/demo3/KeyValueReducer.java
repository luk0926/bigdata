package com.kaikeba.demo3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo3
 * @Author: luk
 * @CreateTime: 2019/12/4 17:58
 */
public class KeyValueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long result = 0;
        for (LongWritable value : values) {
            long l = value.get();

            result += l;
        }

        context.write(key, new LongWritable(result));
    }
}
