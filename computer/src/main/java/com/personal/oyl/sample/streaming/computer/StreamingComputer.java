package com.personal.oyl.sample.streaming.computer;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author OuYang Liang
 * @since 2019-09-03
 */
public class StreamingComputer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 配置并行度
        env.setParallelism(1);

        // 配置checkpoint
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        // 配置state backend
//        env.setStateBackend(((StateBackend)new MemoryStateBackend(5 * 1024 * 1024, false)));
        env.setStateBackend(((StateBackend)new FsStateBackend("file:///Users/john/flinkbackend", false)));

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("order_queue", new SimpleStringSchema(), kafkaConsumeProps());

        DataStream<String> ds = env.addSource(kafkaConsumer);
        DataStream<Order> ods = ds.map(Order::fromJson)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
                    @Override
                    public long extractAscendingTimestamp(Order order) {
                        return order.getOrderTime().getTime();
                    }
                });

        List<HttpHost> httpHosts = esHosts();

        // 计算每分钟下单量、每分钟下单金额、每分钟下单客户数量
        ods.windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
                .allowedLateness(Time.seconds(600)) // Very very important to fault tolerance
                .aggregate(aggregationFunction(), processFunction())
                .addSink(esSinkBuilder(httpHosts, "minute_statistics").build());

        // 计算任意时刻，某客户在5分钟内的下单量、下单金额
        ods.keyBy("custId")
                .window(SlidingEventTimeWindows.of(Time.seconds(300), Time.seconds(60)))
                .allowedLateness(Time.seconds(600))
                .aggregate(userAggregateFunction(), userProcessFunction())
                .addSink(esUserStatisticsSinkBuilder(httpHosts).build());

        // 计算任意时刻，5分钟内下单总量、下单总金额、下单客户数量
        ods.windowAll(SlidingEventTimeWindows.of(Time.seconds(300), Time.seconds(60)))
                .allowedLateness(Time.seconds(600))
                .aggregate(aggregationFunction(), processFunction())
                .addSink(esSinkBuilder(httpHosts, "total_statistics").build());

        env.execute();

    }

    private static Properties kafkaConsumeProps() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink_streaming");
        properties.setProperty("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "false");
        return properties;
    }

    private static List<HttpHost> esHosts() {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        return httpHosts;
    }

    private static ElasticsearchSink.Builder<UserStatistics> esUserStatisticsSinkBuilder(List<HttpHost> httpHosts) {
        ElasticsearchSink.Builder<UserStatistics> builder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<UserStatistics>() {
                    public IndexRequest createIndexRequest(UserStatistics element) {
                        return Requests.indexRequest()
                                .index("user_statistics")
                                .type("doc")
                                .id(element.getMinute() + "-" + element.getCustId())
                                .source(element.json(), XContentType.JSON);
                    }

                    @Override
                    public void process(UserStatistics element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        builder.setBulkFlushMaxActions(1);
        return builder;
    }

    private static ElasticsearchSink.Builder<Statistics> esSinkBuilder(List<HttpHost> httpHosts, final String index) {
        ElasticsearchSink.Builder<Statistics> builder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Statistics>() {
                    public IndexRequest createIndexRequest(Statistics element) {
                        return Requests.indexRequest()
                                .index(index)
                                .type("doc")
                                .id(element.getMinute())
                                .source(element.json(), XContentType.JSON);
                    }

                    @Override
                    public void process(Statistics element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        builder.setBulkFlushMaxActions(1);
        return builder;
    }

    private static AggregateFunction<Order, StatisticsAccumulator, Statistics> aggregationFunction() {
        return new AggregateFunction<Order, StatisticsAccumulator, Statistics>() {
            @Override
            public StatisticsAccumulator createAccumulator() {
                StatisticsAccumulator accumulator = new StatisticsAccumulator();
                accumulator.setOrderAmt(BigDecimal.ZERO);
                accumulator.setNumOfOrders(0L);
                accumulator.setOrderedCustId(new HashSet<>());

                return accumulator;
            }

            @Override
            public StatisticsAccumulator add(Order value, StatisticsAccumulator accumulator) {
                accumulator.setOrderAmt(accumulator.getOrderAmt().add(value.getPayAmt()));
                accumulator.setNumOfOrders(accumulator.getNumOfOrders() + 1);
                accumulator.getOrderedCustId().add(value.getCustId());
                return accumulator;
            }

            @Override
            public Statistics getResult(StatisticsAccumulator accumulator) {
                return accumulator.toStatistics();
            }

            @Override
            public StatisticsAccumulator merge(StatisticsAccumulator a, StatisticsAccumulator b) {
                a.setOrderAmt(a.getOrderAmt().add(b.getOrderAmt()));
                a.setNumOfOrders(a.getNumOfOrders() + b.getNumOfOrders());
                a.getOrderedCustId().addAll(b.getOrderedCustId());
                return a;
            }
        };
    }

    private static AggregateFunction<Order, Map<Integer, UserStatistics>, Map<Integer, UserStatistics>> userAggregateFunction() {
        return new AggregateFunction<Order, Map<Integer, UserStatistics>, Map<Integer, UserStatistics>>() {

            @Override
            public Map<Integer, UserStatistics> createAccumulator() {
                return new HashMap<>();
            }

            @Override
            public Map<Integer, UserStatistics> add(Order value, Map<Integer, UserStatistics> accumulator) {

                UserStatistics statistics = accumulator.get(value.getCustId());
                if (null == statistics) {
                    statistics = new UserStatistics();
                    statistics.setCustId(value.getCustId());
                    statistics.setNumOfOrders(0L);
                    statistics.setOrderAmt(BigDecimal.ZERO);
                }

                statistics.setNumOfOrders(statistics.getNumOfOrders() + 1);
                statistics.setOrderAmt(statistics.getOrderAmt().add(value.getPayAmt()));
                accumulator.put(value.getCustId(), statistics);

                return accumulator;
            }

            @Override
            public Map<Integer, UserStatistics> getResult(Map<Integer, UserStatistics> accumulator) {
                return accumulator;
            }

            @Override
            public Map<Integer, UserStatistics> merge(Map<Integer, UserStatistics> a, Map<Integer, UserStatistics> b) {

                for (Map.Entry<Integer, UserStatistics> entry : b.entrySet()) {
                    if (a.containsKey(entry.getKey())) {
                        UserStatistics piece1 = a.get(entry.getKey());
                        UserStatistics piece2 = entry.getValue();

                        piece1.setNumOfOrders(piece2.getNumOfOrders() + piece1.getNumOfOrders());
                        piece1.setOrderAmt(piece1.getOrderAmt().add(piece2.getOrderAmt()));

                        a.put(entry.getKey(), piece1);
                    } else {
                        a.put(entry.getKey(), entry.getValue());
                    }
                }
                return a;
            }
        };
    }

    private static ProcessAllWindowFunction<Statistics, Statistics, TimeWindow> processFunction() {
        return new ProcessAllWindowFunction<Statistics, Statistics, TimeWindow>() {

            @Override
            public void process(Context context, Iterable<Statistics> elements, Collector<Statistics> out) {
                Statistics statistics = elements.iterator().next();
                statistics.setMinute(format(context.window().getEnd()));
                out.collect(statistics);
            }
        };
    }

    private static ProcessWindowFunction<Map<Integer, UserStatistics>, UserStatistics, Tuple, TimeWindow> userProcessFunction() {
        return new ProcessWindowFunction<Map<Integer, UserStatistics>, UserStatistics, Tuple, TimeWindow>() {

            @Override
            public void process(Tuple tuple, Context context, Iterable<Map<Integer, UserStatistics>> elements, Collector<UserStatistics> out) {
                for (Map.Entry<Integer, UserStatistics> entry : elements.iterator().next().entrySet()) {
                    UserStatistics statistics = entry.getValue();
                    statistics.setMinute(format(context.window().getEnd()));
                    out.collect(statistics);
                }
            }
        };
    }

    private static String format(long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return sdf.format(new Date(timestamp));
    }

}
