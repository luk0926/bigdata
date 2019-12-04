package com.kaikeba.demo3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo3
 * @Author: luk
 * @CreateTime: 2019/12/4 17:54
 */
public class KeyValueMapper extends Mapper<Text, Text, Text, LongWritable> {

    LongWritable outValue = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, outValue);
    }
}
