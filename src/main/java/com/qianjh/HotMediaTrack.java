package com.qianjh;

import com.alibaba.fastjson.JSONObject;
import com.qianjh.domain.LogTrack;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author qianjh
 */
public class HotMediaTrack {

    // 2020-01-18 16:59:58,710 # {"ad_channel_id":465,"ad_id":180,"ad_link_id":3958,"ad_publish_id":116808,"ad_send_code":"078494fa5a21246d","ad_source":"xrm12z","app_vc":"1","app_vn":"1.0","appid":"281572317291614","brand":"Honor","bssid":"24:69:68:33:0a:c0","carrier":"0","cid":"0","click_from":2,"creative_id":26376,"creative_style":8,"delivery_id":148081,"down_x":-999,"down_y":-999,"event_time":1579337996,"event_type":2,"force_pull":100,"imei":"861142031794199","interaction_type":3,"lac":"0","language":"CN","lat":30.417479,"log_time":1579337998710,"lon":113.406448,"mac":"94:fe:22:d1:11:5d","mcc":"0","model":"SCL-TL00","network":"1","network_type":"2","nonce":0.1201206090638749,"os_api_level":"22","os_type":"1","os_version":"5.1.1","pdid":"b08a227d54f7dad8","pkg_name":"com.kub.nyi","psdk_ver":"1807070020","req_id":"4480e506445369_s","req_ip":"117.155.172.197","req_src":"self","screen":"1196*720","screen_density":"320","screen_orientation":"2","timestamp":1579308458,"up_x":-999,"up_y":-999,"usid":"e19d5bf8e18a63931faae516bacb517d"}


    private static final String PARAM_KAFKA_FROM_URL = "kafka_from_url";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameter = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameter);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // kafka配置
        Properties fromProps = new Properties();
        fromProps.setProperty("bootstrap.servers", parameter.getRequired(PARAM_KAFKA_FROM_URL));
        fromProps.setProperty("group.id", "test-flink");
        fromProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        fromProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        fromProps.setProperty("auto.offset.reset", "latest");


        // 从kafka获取 dataSource
        FlinkKafkaConsumer<Tuple2<String, String>> consumer = new FlinkKafkaConsumer<>(
                java.util.regex.Pattern.compile("log_track_\\S+"),
                new TopicValueKafkaDeserializationSchema(),
                fromProps);
        consumer.setStartFromLatest();
        DataStream<Tuple2<String, String>> dataSource = env.addSource(consumer);

        // 转换为bean，指定业务时间
        DataStream<LogTrack> dataStream = dataSource
                .map(new TextToBean())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogTrack>() {
                    @Override
                    public long extractAscendingTimestamp(LogTrack logTrack) {
                        return logTrack.getReceiveTime();
                    }
                });

        //
        dataStream.keyBy(LogTrack::getAppid)
                // 每30秒统计过去十分钟数据
                .timeWindow(Time.minutes(10), Time.seconds(30))
                .aggregate(new CountAgg())
                .keyBy(MediaTrackCount::getWindowEnd)
                .process()
                .print()

        ;


        env.execute("hot items job");
    }

    static class WindowResult implements WindowFunction<Long, MediaTrackCount, Long, TimeWindow> {
        @Override
        public void apply(Long mediaId, TimeWindow timeWindow, Iterable<Long> iterable, Collector<MediaTrackCount> collector) throws Exception {
            collector.collect(MediaTrackCount.builder()
                    .mediaId(mediaId)
                    .windowEnd(timeWindow.getEnd())
                    .count(iterable.iterator().next())
                    .build());
        }
    }

    static class CountAgg implements AggregateFunction<LogTrack, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(LogTrack logTrack, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc0, Long acc1) {
            return acc0 + acc1;
        }
    }

    /**
     * 文本转换为bean
     */
    static class TextToBean implements MapFunction<Tuple2<String, String>, LogTrack> {
        @Override
        public LogTrack map(Tuple2<String, String> x) throws Exception {
            String topic = x.f0;

            String type = topic.replace("log_track_", "");
            String text = x.f1;
            String receiveTimeStr = text.substring(0, 19);
            String jsonStr = text.substring(26);

            JSONObject json = JSONObject.parseObject(jsonStr);

            // appid
            String appid = json.getString("appid");

            // send time
            Long sendTime = json.getLong("timestamp") * 1000;

            // event time
            Long eventTime = json.getLong("event_time");
            if (eventTime != null) {
                eventTime *= 1000L;
            }

            // receive time
            Long receiveTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(receiveTimeStr).getTime();


            return LogTrack.builder()
                    .appid(appid)
                    .type(type)
                    .sendTime(sendTime)
                    .eventTime(eventTime)
                    .receiveTime(receiveTime)
                    .build();
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder(toBuilder = true)
    static class MediaTrackCount {
        private Long mediaId;
        private Long windowEnd;
        private Long count;
    }
}
