package com.kaikeba.demo11_join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo11_join
 * @Author: luk
 * @CreateTime: 2019/12/10 14:04
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdtsMap;

    /**
     * 初始化方法
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pdtsMap = new HashMap<>();
        Configuration configuration = context.getConfiguration();

        //获取到所有的缓存文件，但是现在只有一个缓存文件
        URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);
        //获取到我们放进去的缓存文件
        URI cacheFile = cacheFiles[0];

        //获取FileSystem
        FileSystem fileSystem = FileSystem.get(cacheFile, configuration);
        //读取文件，获取到输入流，这里面是商品的数据
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFile));

        /**
         p0001,xiaomi,1000,2
         p0002,appale,1000,3
         p0003,samsung,1000,4
         */
        //获取到Bufferedreader，之后可以一行一行的读数据
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split(",");
            pdtsMap.put(split[0], line);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        //获取订单表的商品id
        String pid = split[2];

        //获取商品表的数据
        String pdtsLine = pdtsMap.get(pid);

        context.write(new Text(value.toString() + "\t" + pdtsLine), NullWritable.get());
    }
}
