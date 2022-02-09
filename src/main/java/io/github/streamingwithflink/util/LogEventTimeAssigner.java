package io.github.streamingwithflink.util;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class LogEventTimeAssigner implements SerializableTimestampAssigner<LogEvent> {

    public LogEventTimeAssigner(){
    }

    @Override
    public long extractTimestamp(LogEvent logEvent, long l) {
        return logEvent.getTimestamp();
    }
}
