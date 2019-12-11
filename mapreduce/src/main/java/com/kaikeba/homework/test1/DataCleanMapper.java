package com.kaikeba.homework.test1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test1
 * @Author: luk
 * @CreateTime: 2019/12/11 17:34
 */
public class DataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    /*private DataCleanBean dataCleanBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.dataCleanBean = new DataCleanBean();
    }*/

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("DataCleaning", "damagedRecord");

        String[] split = value.toString().split("\t");

        if (split.length != 6){
            counter.increment(1L);
        } else {
            /*dataCleanBean.setDateTime(split[0]);
            dataCleanBean.setUserId(split[1]);
            dataCleanBean.setSearchkwd(split[2]);
            dataCleanBean.setRetorder(split[3]);
            dataCleanBean.setCliorder(split[4]);
            dataCleanBean.setCliurl(split[5]);*/

            context.write(new Text(value), NullWritable.get());
        }
    }
}
