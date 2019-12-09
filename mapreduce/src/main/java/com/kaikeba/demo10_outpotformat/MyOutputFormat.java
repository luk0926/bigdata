package com.kaikeba.demo10_outpotformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo10_outpotformat
 * @Author: luk
 * @CreateTime: 2019/12/9 15:48
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);

        Path goodPath = new Path("file:///D:\\资料\\开课吧\\8_hadoop\\1206-课后资料-mapreduce（三)\\10、自定义outputFormat\\数据\\\\goodComment\\\\mygood.txt");
        Path badPath = new Path("file:///D:\\资料\\开课吧\\8_hadoop\\1206-课后资料-mapreduce（三)\\10、自定义outputFormat\\数据\\\\badCOmment\\\\mybad.txt");

        FSDataOutputStream goodStream = fileSystem.create(goodPath);
        FSDataOutputStream basdStream = fileSystem.create(badPath);

        MyRecordWriter myRecordWriter = new MyRecordWriter(goodStream, basdStream);
        return myRecordWriter;
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable> {
        //将数据往hdfs上面写  FSDataOutputStream

        FSDataOutputStream goodStream = null;
        FSDataOutputStream badStream = null;

        public MyRecordWriter(FSDataOutputStream goodStream, FSDataOutputStream badStream) {
            this.goodStream = goodStream;
            this.badStream = badStream;
        }

        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            String commentStatus = text.toString().split("\t")[9];
            if (commentStatus.equals("0")) {
                goodStream.write(text.toString().getBytes());
                goodStream.write("\r\n".getBytes());
            }else{
                badStream.write(text.toString().getBytes());
                badStream.write("\r\n".getBytes());
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            IOUtils.closeStream(goodStream);
            IOUtils.closeStream(badStream);
        }
    }
}
