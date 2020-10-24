package org.apache.flink.training.assignments.sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public interface SinkContainer {
    SinkFunction RETURN_SINK= new CollectReturnSink();
    SinkFunction AVERAGE_SINK= new CollectAverageSink();
    SinkFunction VOLATILITY_SINK= new CollectVolatilitySink();
}
