/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qianjh;

import com.qianjh.domain.ItemViewCount;
import com.qianjh.domain.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;


/**
 * @author qianjh
 */
public class HotItemAnalysis {

    /**
     * userId, itemId, categoryId, behavior, timestamp
     */
    private static final String FILE_PATH = "/home/qianjh/Mock/UserBehavior.csv";
    private static final String PARAM_KAFKA_FROM_URL = "kafka_from_url";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameter = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameter);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties fromProps = new Properties();
        fromProps.setProperty("bootstrap.servers", parameter.getRequired(PARAM_KAFKA_FROM_URL));
        fromProps.setProperty("group.id", "test-flink");
        fromProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        fromProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        fromProps.setProperty("auto.offset.reset", "latest");

        // 从文件获取
//        DataStream<String> dataSource = env.readTextFile(FILE_PATH);
        // 从kafka获取
        DataStream<String> dataSource = env.addSource(new FlinkKafkaConsumer<>("hot-items", new SimpleStringSchema(), fromProps));

        // 转换成pojo数据流
        DataStream<UserBehavior> dataStream = dataSource
                .map(x -> {
                    String[] s = x.split(",");

                    return UserBehavior.builder()
                            .userId(Long.valueOf(s[0].trim()))
                            .itemId(Long.valueOf(s[1].trim()))
                            .categoryId(Long.valueOf(s[2].trim()))
                            .behavior(s[3].trim())
                            .timestamp(Long.valueOf(s[4].trim()))
                            .build();
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000;
                    }
                });

        //
        dataStream
                .filter((FilterFunction<UserBehavior>) x -> x.getBehavior().equals("pv"))
                .keyBy(UserBehavior::getItemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(3))
                .print()
        ;

        env.execute("hot items job");
    }


    static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }


    class AverageAgg implements AggregateFunction<UserBehavior, Tuple2<Long, Integer>, BigDecimal> {

        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            return Tuple2.of(0L, 0);
        }

        @Override
        public Tuple2<Long, Integer> add(UserBehavior userBehavior, Tuple2<Long, Integer> tuple2) {
            return Tuple2.of(tuple2.f0 + userBehavior.getTimestamp(), tuple2.f1 + 1);
        }

        @Override
        public BigDecimal getResult(Tuple2<Long, Integer> tuple2) {
            return new BigDecimal(tuple2.f0).divide(new BigDecimal(tuple2.f1));
        }

        @Override
        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> acc0, Tuple2<Long, Integer> acc1) {
            return Tuple2.of(acc0.f0 + acc1.f0, acc0.f1 + acc1.f1);
        }
    }

    static class WindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
        @Override
        public void apply(Long key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> out) throws Exception {
            out.collect(ItemViewCount.builder()
                    .itemId(key)
                    .windowEnd(timeWindow.getEnd())
                    .count(iterable.iterator().next())
                    .build());
        }
    }

    static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private Integer topSize;
        private ListState<ItemViewCount> itemState;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemState = getRuntimeContext().getListState(new ListStateDescriptor<>("item-state", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            itemState.add(itemViewCount);
            // 注册定时器
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 定时器触发时，对所有数据排序并输出结果
            List<ItemViewCount> items = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                items.add(item);
            }

            items.sort(Comparator.comparingLong(ItemViewCount::getCount).reversed());
            StringBuilder buffer = new StringBuilder()
                    .append("time : ")
                    .append(new Timestamp(timestamp - 1))
                    .append("\n");

            for (int i = 0; i < items.size(); i++) {
                if (i + 1 > topSize) {
                    break;
                }
                ItemViewCount item = items.get(i);
                buffer.append("No ").append(i + 1).append(" -> ").append(item.getItemId()).append(" : ").append(item.getCount()).append("\n");
            }
            buffer.append("=============================");


            out.collect(buffer.toString());

            itemState.clear();
        }
    }
}
