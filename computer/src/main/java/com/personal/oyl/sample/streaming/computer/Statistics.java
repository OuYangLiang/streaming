package com.personal.oyl.sample.streaming.computer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @author OuYang Liang
 * @since 2019-09-03
 */
public class Statistics implements Serializable {

    private transient static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private String minute;
    private Long numOfOrders;
    private BigDecimal numOfOrderAmt;
    private Long numOfOrderedCustomers;

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

    public Long getNumOfOrderedCustomers() {
        return numOfOrderedCustomers;
    }

    public void setNumOfOrderedCustomers(Long numOfOrderedCustomers) {
        this.numOfOrderedCustomers = numOfOrderedCustomers;
    }

    public String json() {
        return gson.toJson(this);
    }

    public static Statistics fromJson(String json) {
        return gson.fromJson(json, Statistics.class);
    }
}
