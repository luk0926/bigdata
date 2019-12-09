package com.kaikeba.demo9_topN;

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
 * @BelongsPackage: com.kaikeba.demo8_group
 * @Author: luk
 * @CreateTime: 2019/12/9 14:38
 */
public class GroupMain extends Configured implements Tool {

    /**
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "GroupMain");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\9、mapreduce当中的分组求topN\\数据"));

        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //分区
        job.setPartitionerClass(GrouperPartitioner.class);

        //排序（已经做了）

        //规约（省略）

        //分组
        job.setGroupingComparatorClass(MyGroup.class);

        job.setReducerClass(GroupReduce.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1202_课前资料_mr与yarn\\3、第三天\\9、mapreduce当中的分组求topN\\out"));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        int run = ToolRunner.run(configuration, new GroupMain(), args);
        System.exit(run);
    }
}
