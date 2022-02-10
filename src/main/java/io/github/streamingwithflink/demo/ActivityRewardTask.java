package io.github.streamingwithflink.demo;

import com.alibaba.fastjson.JSON;
import io.github.streamingwithflink.util.LogEvent;
import io.github.streamingwithflink.util.LogEventTimeAssigner;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class ActivityRewardTask {
    // {"eventType":"register", "orgId":11, "parentOrgId":10, "timestamp":1644410510000}
    // {"eventType":"order", "orgId":11, "parentOrgId":10, "timestamp":1644410510001}
    // {"eventType":"register", "orgId":12, "parentOrgId":10, "timestamp":1644410510002}
    // {"eventType":"order", "orgId":12, "parentOrgId":10, "timestamp":1644410510003}
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "GID_ACTIVITY");

        WatermarkStrategy<LogEvent> watermarkStrategy = WatermarkStrategy
                .<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner(new LogEventTimeAssigner());

        DataStream<String> logEventDataStream = env
                .addSource(new FlinkKafkaConsumer<>("log_event", new SimpleStringSchema(), properties))
                .map(x -> JSON.parseObject(x, LogEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(LogEvent::getParentOrgId)
                .process(new RuleProcessorFunction())
                .setParallelism(10)
                .map(JSON::toJSONString);

        logEventDataStream.addSink(new FlinkKafkaProducer<String>("reward_event", new SimpleStringSchema(), properties));
        env.execute("activity reward");
    }

    public static class RuleProcessorFunction extends KeyedProcessFunction<Integer, LogEvent, Integer> {

        ListState<LogEvent> registerStateList;
        ListState<LogEvent> orderStateList;
        ListState<Integer> countedOrgStateList;
        ValueState<Integer> inviteCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            registerStateList = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("registerStateList", LogEvent.class));

            orderStateList = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("orderStateList", LogEvent.class));

            countedOrgStateList = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("countedOrgStateList", Integer.class));

            inviteCountState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("inviteCountState", Integer.class));
        }

        @Override
        public void processElement(LogEvent logEvent, KeyedProcessFunction<Integer, LogEvent, Integer>.Context context, Collector<Integer> collector) throws Exception {

            boolean isMatchedOrg = false;
            List<LogEvent> logEventList = Lists.newArrayList();
            if ("register".equalsIgnoreCase(logEvent.getEventType())) {
                registerStateList.add(logEvent);
                logEventList = Lists.newArrayList(orderStateList.get().iterator());

            } else if ("order".equalsIgnoreCase(logEvent.getEventType())) {
                orderStateList.add(logEvent);
                logEventList = Lists.newArrayList(registerStateList.get().iterator());
            }

            isMatchedOrg = logEventList.stream().anyMatch(x -> x.getOrgId().equals(logEvent.getOrgId()));
            if (isMatchedOrg) {
                List<Integer> countedOrgList = Lists.newArrayList(countedOrgStateList.get().iterator());
                boolean isNotCounted = countedOrgList.stream().noneMatch(x -> x.equals(logEvent.getParentOrgId()));
                Integer inviteCount = inviteCountState.value();
                inviteCount = Objects.isNull(inviteCount) ? 0 : inviteCount;
                if (isNotCounted) {
                    inviteCount = inviteCount + 1;
                    if (inviteCount >= 2) {
                        registerStateList.clear();
                        orderStateList.clear();
                        collector.collect(logEvent.getParentOrgId());
                    }

                    inviteCountState.update(inviteCount);

                }
            }
        }
    }
}