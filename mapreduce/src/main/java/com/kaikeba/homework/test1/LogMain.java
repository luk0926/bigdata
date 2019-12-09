package com.kaikeba.homework.test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test1
 * @Author: luk
 * @CreateTime: 2019/12/9 18:07
 */
public class LogMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "LogMain");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1207_作业mapreduce\\mapreduce作业\\data"));

        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(LogBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(LogReduce.class);
        job.setOutputKeyClass(LogBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1207_作业mapreduce\\mapreduce作业\\out"));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new LogMain(), args);
        System.exit(run);
    }
}
