package com.kaikeba.demo7_combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo7_combine
 * @Author: luk
 * @CreateTime: 2019/12/5 23:37
 */
public class WordCountMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "WordCountMain");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("E:\\开课吧\\08_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\1、wordCount_input\\数据"));

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //规约
        job.setCombinerClass(MyCombiner.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("E:\\开课吧\\08_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\1、wordCount_input\\out"));

        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int run = ToolRunner.run(conf, new WordCountMain(), args);
        System.exit(run);
    }
}
