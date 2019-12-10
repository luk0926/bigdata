package com.kaikeba.homework.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test
 * @Author: luk
 * @CreateTime: 2019/12/10 11:35
 */
public class LogMapper extends Mapper<LongWritable, Text, LogBean, NullWritable> {

    private LogBean logBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.logBean = new LogBean();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Counter counter = context.getCounter("COUNT", "MapRecordCounter");

        String[] split = value.toString().split("\t");
        int length = split.length;

        String dateTime = split[0];
        String userId = split[1];
        String searchkwd = split[2];
        String retorder = split[3];
        String cliorder = split[4];
        String cliurl = split[5];


        logBean.setDateTime(Long.parseLong(dateTime));
        logBean.setUserId(userId);
        logBean.setSearchkwd(searchkwd);
        logBean.setRetorder(Integer.parseInt(retorder));
        logBean.setCliorder(Integer.parseInt(cliorder));
        logBean.setCliurl(cliurl);

        context.write(logBean, NullWritable.get());
    }
}