package com.kaikeba.demo7_combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo7_combine
 * @Author: luk
 * @CreateTime: 2019/12/5 23:28
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k2;
    private IntWritable v2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k2 = new Text();
        v2 = new IntWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");

        for (String word : split) {
            k2.set(word);
            v2.set(1);
        }

        context.write(k2, v2);
    }
}
