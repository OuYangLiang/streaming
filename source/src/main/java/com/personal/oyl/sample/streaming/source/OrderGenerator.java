package com.personal.oyl.sample.streaming.source;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author OuYang Liang
 * @since 2019-09-04
 */
public class OrderGenerator {

    private static int orderSeq = 1000001;
    private Random intGen = new Random(37);

    private int getCustId() {
        return intGen.nextInt(1000) + 1;
    }

    private String getProdCode() {
        return Integer.toString(intGen.nextInt(100) + 1);
    }

    private BigDecimal getPayAmt() {
        return BigDecimal.valueOf(intGen.nextInt(4901) + 100);
    }

    private long getOrderTime(Date time) {
        Calendar c = Calendar.getInstance();
        c.setTime(time);
        c.set(Calendar.MINUTE, -30);
        return c.getTimeInMillis();
    }

    private long getPayTime(Date time) {
        Calendar c = Calendar.getInstance();
        c.setTime(time);
        c.set(Calendar.MINUTE, -20);
        return c.getTimeInMillis();
    }

    public Order next() {
        Order rlt = new Order();
        rlt.setOrderId(OrderGenerator.orderSeq++);
        rlt.setCustId(this.getCustId());
        rlt.setProductCode(this.getProdCode());
        rlt.setPayAmt(this.getPayAmt());
        rlt.setDiscount(BigDecimal.valueOf(intGen.nextInt(((int) (rlt.getPayAmt().intValue() * 0.1)))));
        rlt.setTotalAmt(rlt.getPayAmt().add(rlt.getDiscount()));

        Date now = new Date();
        rlt.setOrderTime(this.getOrderTime(now));
        rlt.setPayTime(this.getPayTime(now));
        return rlt;
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        List<Future<RecordMetadata>> futures = new LinkedList<>();

        OrderGenerator gen = new OrderGenerator();
        while (!Thread.currentThread().isInterrupted()) {
            Order order = gen.next();
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "order_queue", order.getCustId() & 15, null, null, order.json(), null);
            futures.add(producer.send(record));

            if (futures.size() == 100) {
                for(Future<RecordMetadata> future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                        break;
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
