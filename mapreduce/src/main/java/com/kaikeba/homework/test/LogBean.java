package com.kaikeba.homework.test;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test
 * @Author: luk
 * @CreateTime: 2019/12/10 11:35
 */
public class LogBean implements Writable {
    private Long dateTime;
    private String userId;
    private String searchkwd;
    private int retorder;
    private int cliorder;
    private String cliurl;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dateTime);
        out.writeUTF(userId);
        out.writeUTF(searchkwd);
        out.writeInt(retorder);
        out.writeInt(cliorder);
        out.writeUTF(cliurl);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateTime = in.readLong();
        this.userId = in.readUTF();
        this.searchkwd = in.readUTF();
        this.retorder = in.readInt();
        this.cliorder = in.readInt();
        this.cliurl = in.readUTF();
    }

    public Long getDateTime() {
        return dateTime;
    }

    public void setDateTime(Long dateTime) {
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

    public int getRetorder() {
        return retorder;
    }

    public void setRetorder(int retorder) {
        this.retorder = retorder;
    }

    public int getCliorder() {
        return cliorder;
    }

    public void setCliorder(int cliorder) {
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
        return "LogBean{" +
                "dateTime=" + dateTime +
                ", userId='" + userId + '\'' +
                ", searchkwd='" + searchkwd + '\'' +
                ", retorder=" + retorder +
                ", cliorder=" + cliorder +
                ", cliurl='" + cliurl + '\'' +
                '}';
    }


}
