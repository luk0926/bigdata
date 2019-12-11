package com.kaikeba.homework.test1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test1
 * @Author: luk
 * @CreateTime: 2019/12/11 17:30
 */
public class DataCleanBean implements Writable {
    private String dateTime;
    private String userId;
    private String searchkwd;
    private String retorder;
    private String cliorder;
    private String cliurl;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(dateTime);
        out.writeUTF(userId);
        out.writeUTF(searchkwd);
        out.writeUTF(retorder);
        out.writeUTF(cliorder);
        out.writeUTF(cliurl);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateTime = in.readUTF();
        this.userId = in.readUTF();
        this.searchkwd = in.readUTF();
        this.retorder = in.readUTF();
        this.cliorder = in.readUTF();
        this.cliurl = in.readUTF();
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSearchkwd() {
        return searchkwd;
    }

    public void setSearchkwd(String searchkwd) {
        this.searchkwd = searchkwd;
    }

    public String getRetorder() {
        return retorder;
    }

    public void setRetorder(String retorder) {
        this.retorder = retorder;
    }

    public String getCliorder() {
        return cliorder;
    }

    public void setCliorder(String cliorder) {
        this.cliorder = cliorder;
    }

    public String getCliurl() {
        return cliurl;
    }

    public void setCliurl(String cliurl) {
        this.cliurl = cliurl;
    }

    @Override
    public String toString() {
        return "DataCleanBean{" +
                "dateTime='" + dateTime + '\'' +
                ", userId='" + userId + '\'' +
                ", searchkwd='" + searchkwd + '\'' +
                ", retorder='" + retorder + '\'' +
                ", cliorder='" + cliorder + '\'' +
                ", cliurl='" + cliurl + '\'' +
                '}';
    }
}
