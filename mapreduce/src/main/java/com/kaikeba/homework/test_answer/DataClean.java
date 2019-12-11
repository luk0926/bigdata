package com.kaikeba.homework.test_answer;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test_answer
 * @Author: luk
 * @CreateTime: 2019/12/11 17:13
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * 现对sogou日志数据，做数据清洗；将不符合格式要求的数据删除
 * 每行记录有6个字段；
 * 分别表示时间datetime、用户ID userid、新闻搜索关键字searchkwd、当前记录在返回列表中的序号retorder、用户点击链接的顺序cliorder、点击的URL连接cliurl
 * 日志样本：
 * 20111230111308  0bf5778fc7ba35e657ee88b25984c6e9        nba直播 4       1       http://www.hoopchina.com/tv
 *
 */
public class DataClean  {

    /**
     *
     * 基本上大部分MR程序的main方法逻辑，大同小异；将其他MR程序的main方法代码拷贝过来，稍做修改即可
     * 实际开发中，也会有很多的复制、粘贴、修改
     *
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //判断一下，输入参数是否是两个，分别表示输入路径、输出路径
        /*if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }*/

        Configuration configuration = new Configuration();

        //调用getInstance方法，生成job实例
        Job job = Job.getInstance(configuration, DataClean.class.getSimpleName());

        //设置jar包，参数是包含main方法的类
        job.setJarByClass(DataClean.class);

        //设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1207_作业mapreduce\\mapreduce作业\\data"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\资料\\开课吧\\8_hadoop\\1207_作业mapreduce\\mapreduce作业\\out"));

        //设置处理Map阶段的自定义的类
        job.setMapperClass(DataCleanMapper.class);

        //注意：此处设置的map输出的key/value类型，一定要与自定义map类输出的kv对类型一致；否则程序运行报错
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //注意：因为不需要reduce聚合阶段，所以，需要显示设置reduce task个数是0
        job.setNumReduceTasks(0);

        // 提交作业
        job.waitForCompletion(true);
    }

    /**
     *
     * 自定义mapper类
     * 注意：若自定义的mapper类，与main方法在同一个类中，需要将自定义mapper类，声明成static的
     */
    public static class DataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        //为了提高程序的效率，避免创建大量短周期的对象，出发频繁GC；此处生成一个对象，共用
        NullWritable nullValue = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //自定义计数器，用于记录残缺记录数
            Counter counter = context.getCounter("DataCleaning", "damagedRecord");

            //获得当前行数据
            //样例数据：20111230111645  169796ae819ae8b32668662bb99b6c2d        塘承高速公路规划线路图  1       1       http://auto.ifeng.com/roll/20111212/729164.shtml
            String line = value.toString();

            //将行数据按照记录中，字段分隔符切分
            String[] fields = line.split("\t");

            //判断字段数组长度，是否为6
            if(fields.length != 6) {
                //若不是，则不输出，并递增自定义计数器
                counter.increment(1L);
            } else {
                //若是6，则原样输出
                context.write(value, nullValue);
            }
        }
    }
}
