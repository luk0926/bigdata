package com.kaikeba.demo4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo4
 * @Author: luk
 * @CreateTime: 2019/12/4 18:39
 */
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    /**
     * 读取文件
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        MyRecordReader myRecordReader = new MyRecordReader();
        myRecordReader.initialize(inputSplit, taskAttemptContext);
        return myRecordReader;
    }

    /**
     * 注意这个方法，决定我们的文件是否可以切分，如果不可切分，直接返回false
     * 到时候读取数据的时候，一次性将文件内容全部都读取出来
     *
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false; //文件不可切分
    }
}
