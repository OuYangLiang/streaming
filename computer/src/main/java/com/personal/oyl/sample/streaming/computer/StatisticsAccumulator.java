package com.personal.oyl.sample.streaming.computer;

import java.math.BigDecimal;
import java.util.Set;

/**
 * @author OuYang Liang
 * @since 2019-09-04
 */
public class StatisticsAccumulator {
    private Long numOfOrders;
    private BigDecimal numOfOrderAmt;
    private Set<Integer> orderedCustId;

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

    public Set<Integer> getOrderedCustId() {
        return orderedCustId;
    }

    public void setOrderedCustId(Set<Integer> orderedCustId) {
        this.orderedCustId = orderedCustId;
    }

    public Statistics toStatistics() {
        Statistics statistics = new Statistics();
        statistics.setNumOfOrderAmt(this.getNumOfOrderAmt());
        statistics.setNumOfOrders(this.getNumOfOrders());
        if (null != this.getOrderedCustId()) {
            statistics.setNumOfOrderedCustomers(Long.valueOf(this.getOrderedCustId().size()));
        } else {
            statistics.setNumOfOrderedCustomers(0L);
        }
        return statistics;
    }
}
