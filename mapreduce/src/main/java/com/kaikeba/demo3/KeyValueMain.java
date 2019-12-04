package com.kaikeba.demo3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo3
 * @Author: luk
 * @CreateTime: 2019/12/4 18:19
 */
public class KeyValueMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        //翻阅 KeyValueLineRecordReader 的源码，发现切割参数的配置
        configuration.set("key.value.separator.in.input.line", "@zolen@");

        Job job = Job.getInstance(configuration);

        KeyValueTextInputFormat.addInputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\2、keyValueTextInputFormat\\数据"));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(KeyValueMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(KeyValueReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //输出数据
        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\2、keyValueTextInputFormat\\out"));

        boolean b = job.waitForCompletion(true);

        System.exit(0);
    }

}
