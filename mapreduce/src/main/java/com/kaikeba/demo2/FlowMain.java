package com.kaikeba.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo2
 * @Author: luk
 * @CreateTime: 2019/12/4 17:25
 */
public class FlowMain extends Configured implements Tool {

    /**
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        //获取job对象
        Job job = Job.getInstance(super.getConf(), "FlowMain");

        //如果程序打包运行必须要设置这一句
        //job.setJarByClass(FlowMain.class);

        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\2、hadoop的序列化\\数据\\input"));

        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\2、hadoop的序列化\\数据\\out"));

        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int run = ToolRunner.run(conf, new FlowMain(), args);

        System.exit(run);
    }
}
