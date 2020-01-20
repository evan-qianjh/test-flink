package com.qianjh;

import com.alibaba.fastjson.JSONObject;
import com.qianjh.domain.LogTrack;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * @author qianjh
 */
public class HotMediaTrack {

    // 2020-01-18 16:59:58,710 # {"ad_channel_id":465,"ad_id":180,"ad_link_id":3958,"ad_publish_id":116808,"ad_send_code":"078494fa5a21246d","ad_source":"xrm12z","app_vc":"1","app_vn":"1.0","appid":"281572317291614","brand":"Honor","bssid":"24:69:68:33:0a:c0","carrier":"0","cid":"0","click_from":2,"creative_id":26376,"creative_style":8,"delivery_id":148081,"down_x":-999,"down_y":-999,"event_time":1579337996,"event_type":2,"force_pull":100,"imei":"861142031794199","interaction_type":3,"lac":"0","language":"CN","lat":30.417479,"log_time":1579337998710,"lon":113.406448,"mac":"94:fe:22:d1:11:5d","mcc":"0","model":"SCL-TL00","network":"1","network_type":"2","nonce":0.1201206090638749,"os_api_level":"22","os_type":"1","os_version":"5.1.1","pdid":"b08a227d54f7dad8","pkg_name":"com.kub.nyi","psdk_ver":"1807070020","req_id":"4480e506445369_s","req_ip":"117.155.172.197","req_src":"self","screen":"1196*720","screen_density":"320","screen_orientation":"2","timestamp":1579308458,"up_x":-999,"up_y":-999,"usid":"e19d5bf8e18a63931faae516bacb517d"}

    private static final String PARAM_KAFKA_URL = "kafka_url";
    private static final String PARAM_MYSQL_URL = "mysql_url";
    private static final String PARAM_MYSQL_USERNAME = "mysql_username";
    private static final String PARAM_MYSQL_PASSWORD = "mysql_password";

    public static void main(String[] args) throws Exception {
        // 初始化
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameter = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameter);

        // 设置并行度 和 事件时间
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 获取参数
        String kafkaUrl = parameter.getRequired(PARAM_KAFKA_URL);

        // kafka配置
        Properties fromProps = new Properties();
        fromProps.setProperty("bootstrap.servers", kafkaUrl);
        fromProps.setProperty("group.id", "test-flink");
        fromProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        fromProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        fromProps.setProperty("auto.offset.reset", "latest");

        // 从kafka获取 dataSource
        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>(
                java.util.regex.Pattern.compile("log_track_\\S+"),
                new SimpleStringSchema(),
                fromProps);
        consumer.setStartFromLatest();


        DataStream<String> dataSource = env.addSource(consumer);

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
        dataStream
                .keyBy("appid", "type")
                .timeWindow(Time.minutes(5), Time.seconds(30))
                .aggregate(new CountAgg(), new WindowResult())
                .addSink(new MySqlSink())
        ;


        env.execute("track count");
    }

    static class MySqlSink extends RichSinkFunction<LogTrackPoint> {
        private Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ParameterTool parameter = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

            String url = parameter.get(PARAM_MYSQL_URL);
            String username = parameter.get(PARAM_MYSQL_USERNAME);
            String password = parameter.get(PARAM_MYSQL_PASSWORD);

            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, username, password);
        }

        @Override
        public void invoke(LogTrackPoint value, Context context) throws Exception {
            String sql = "insert into log_track_point (`time`, `appid`, `type`, `count`) value (?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setTimestamp(1, new Timestamp(value.getWindowEnd()));
            preparedStatement.setString(2, value.getAppid());
            preparedStatement.setInt(3, value.getType());
            preparedStatement.setLong(4, value.getCount());
            preparedStatement.execute();
            preparedStatement.close();
        }

        @Override
        public void close() throws Exception {
            super.close();
            connection.close();
        }
    }


    static class WindowResult implements WindowFunction<Long, LogTrackPoint, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<LogTrackPoint> collector) throws Exception {
            Tuple2<String, Integer> appidAndType = (Tuple2<String, Integer>) tuple;

            collector.collect(LogTrackPoint.builder()
                    .appid(appidAndType.f0)
                    .type(appidAndType.f1)
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
    static class TextToBean implements MapFunction<String, LogTrack> {

        @Override
        public LogTrack map(String text) throws Exception {
            JSONObject json = JSONObject.parseObject(text);

            // appid
            String appid = json.getString("appid");

            // 下发时间
            Long sendTime = json.getLong("timestamp") * 1000;

            // 事件时间
            Long eventTime = json.getLong("event_time");
            if (eventTime != null) {
                eventTime *= 1000L;
            }

            // 上报时间
            Long receiveTime = json.getLong("log_time");

            // track类型
            Integer trackType = json.getInteger("event_type");


            return LogTrack.builder()
                    .appid(appid)
                    .type(trackType)
                    .sendTime(sendTime)
                    .eventTime(eventTime)
                    .receiveTime(receiveTime)
                    .build();
        }
    }

    /**
     * 日志保存点
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder(toBuilder = true)
    static class LogTrackPoint {
        private String appid;
        private Integer type;
        private Long windowEnd;
        private Long count;
    }

}
