package com.kaikeba.demo11_join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo11_join
 * @Author: luk
 * @CreateTime: 2019/12/10 14:18
 */
public class MapJoinMain extends Configured implements Tool {

    /**
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        URI uri = new URI("hdfs://node01:8020/cache/pdts.txt");
        Job job = Job.getInstance(super.getConf(), "MapJoinMain");

        //添加缓存文件
        DistributedCache.addCacheFile(uri, super.getConf());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(""));
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //没有reducer逻辑，不用设置了
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(""));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        int run = ToolRunner.run(configuration, new MapJoinMain(), args);
        System.exit(run);
    }
}
