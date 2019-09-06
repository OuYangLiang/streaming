package com.personal.oyl.sample.streaming.computer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @author OuYang Liang
 * @since 2019-09-03
 */
public class UserStatistics implements Serializable {
    private transient static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private Integer custId;
    private String minute;
    private Long numOfOrders;
    private BigDecimal numOfOrderAmt;

    public Integer getCustId() {
        return custId;
    }

    public void setCustId(Integer custId) {
        this.custId = custId;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
    }

    public Long getNumOfOrders() {
        return numOfOrders;
    }

    public void setNumOfOrders(Long numOfOrders) {
        this.numOfOrders = numOfOrders;
    }

    public BigDecimal getNumOfOrderAmt() {
        return numOfOrderAmt;
    }

    public void setNumOfOrderAmt(BigDecimal numOfOrderAmt) {
        this.numOfOrderAmt = numOfOrderAmt;
    }

    public String json() {
        return gson.toJson(this);
    }

    public static UserStatistics fromJson(String json) {
        return gson.fromJson(json, UserStatistics.class);
    }
}
