package com.kaikeba;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2019/12/3 21:22
 */
public class HdfsOperatot {

    /**
     *
     * @throws IOException
     */
    @Test
    public void mkdirToHdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("/kaikeba/dir1"));
        fileSystem.close();
    }

}
