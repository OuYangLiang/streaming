package com.personal.oyl.sample.streaming.source;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @author OuYang Liang
 * @since 2019-09-05
 */
public final class OrderPersister {

    private static final String sql = "insert into `order`(order_id, prod_code, pay_amt, discount, total_amt, cust_id, order_time, pay_time) values(?,?,?,?,?,?,?,?);";

    private volatile static Connection conn = null;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void open() throws SQLException {
        if (null == conn) {
            synchronized (OrderPersister.class) {
                if (null == conn) {
                    conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/streaming?characterEncoding=UTF-8", "root", "password");
                }
            }
        }
    }

    public void close() throws SQLException {
        if (null != conn) {
            conn.close();
        }
    }

    public void batchSave(List<Order> orders) throws SQLException {
        this.open();
        conn.setAutoCommit(false);
        PreparedStatement p = null;

        try {
            p = conn.prepareStatement(sql);

            for (Order order : orders) {
                p.setInt(1, order.getOrderId());
                p.setString(2, order.getProductCode());
                p.setBigDecimal(3, order.getPayAmt());
                p.setBigDecimal(4, order.getDiscount());
                p.setBigDecimal(5, order.getTotalAmt());
                p.setInt(6, order.getCustId());
                p.setTimestamp(7, new Timestamp(order.getOrderTime()));
                p.setTimestamp(8, new Timestamp(order.getPayTime()));

                p.executeUpdate();
            }

            conn.commit();
        } finally {
            if (null != p) {
                p.close();
            }
        }

    }

    private void save(Order order) throws SQLException {
        this.open();
        conn.setAutoCommit(false);
        PreparedStatement p = null;

        try {
            p = conn.prepareStatement(sql);

            p.setInt(1, order.getOrderId());
            p.setString(2, order.getProductCode());
            p.setBigDecimal(3, order.getPayAmt());
            p.setBigDecimal(4, order.getDiscount());
            p.setBigDecimal(5, order.getTotalAmt());
            p.setInt(6, order.getCustId());
            p.setTimestamp(7, new Timestamp(order.getOrderTime()));
            p.setTimestamp(8, new Timestamp(order.getPayTime()));

            p.executeUpdate();
            conn.commit();
        } finally {
            if (null != p) {
                p.close();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        OrderGenerator gen = new OrderGenerator();
        List<Order> orders = new LinkedList<>();

        for (int i = 1; i <= 1000; i++) {
            orders.add(gen.next());
        }

        System.out.println(System.currentTimeMillis());
        new OrderPersister().batchSave(orders);
        System.out.println(System.currentTimeMillis());

    }
}
