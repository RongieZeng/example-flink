package io.github.streamingwithflink.util;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class SensorTimeAssignerNew implements SerializableTimestampAssigner<SensorReading> {

    public SensorTimeAssignerNew(){
    }

    @Override
    public long extractTimestamp(SensorReading sensorReading, long l) {
        return sensorReading.timestamp;
    }
}
