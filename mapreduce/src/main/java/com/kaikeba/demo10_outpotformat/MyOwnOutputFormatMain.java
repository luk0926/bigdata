package com.kaikeba.demo10_outpotformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo10_outpotformat
 * @Author: luk
 * @CreateTime: 2019/12/9 16:17
 */
public class MyOwnOutputFormatMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "MyOwnOutputFormatMain");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1206-课后资料-mapreduce（三)\\10、自定义outputFormat\\数据\\input"));
        job.setMapperClass(MyOwnMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(MyOutputFormat.class);
        //设置一个输出目录，这个目录会输出一个success的成功标志的文件
        MyOutputFormat.setOutputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1206-课后资料-mapreduce（三)\\10、自定义outputFormat\\数据\\outputResult"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //设置我们的reduce阶段的压缩
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");



        int run = ToolRunner.run(configuration, new MyOwnOutputFormatMain(), args);
        System.exit(run);
    }

    public static class MyOwnMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String commentStatus = split[9];

            context.write(value, NullWritable.get());
        }
    }
}
