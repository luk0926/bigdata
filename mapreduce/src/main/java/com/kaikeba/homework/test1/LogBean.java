package com.kaikeba.homework.test1;

import java.util.Date;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.test
 * @Author: luk
 * @CreateTime: 2019/12/9 17:46
 */
public class LogBean {

    private Long dateTime;
    private String userId;
    private String searchkwd;
    private int retorder;
    private int cliorder;
    private String cliurl;

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
