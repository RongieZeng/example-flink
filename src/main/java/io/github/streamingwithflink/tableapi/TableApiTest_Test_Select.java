package io.github.streamingwithflink.tableapi;

import com.alibaba.fastjson.JSON;
import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssignerNew;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiTest_Test_Select {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "GID_MARKETING");
        // ingest sensor stream
        WatermarkStrategy<SensorReading> watermarkStrategy = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SensorTimeAssignerNew())
                .withIdleness(Duration.ofMinutes(1));
        DataStream<SensorReading> readings = env
                // SensorSource generates random temperature readings
//                .addSource(new SensorSource())
                .addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties))
                // assign timestamps and watermarks which are required for event time
                .map(x -> JSON.parseObject(x, SensorReading.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);
// {"id":"sensor_1","timestamp":1234567890123,"temperature":23.6}
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. table api
        Table table = tableEnv.fromDataStream(readings);
        Table tableResult = table.select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_1"));

        tableEnv.toDataStream(tableResult).print().name("table");

        // 2. sql
        tableEnv.createTemporaryView("sensor", readings);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table sqlResult = tableEnv.sqlQuery(sql);

        tableEnv.toDataStream(sqlResult).print().name("sql");

        env.execute();
    }
}
