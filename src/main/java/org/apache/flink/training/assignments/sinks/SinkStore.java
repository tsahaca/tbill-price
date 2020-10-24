package org.apache.flink.training.assignments.sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkStore {
    private SinkFunction returnSink;
    private SinkFunction averageSink;
    private SinkFunction volatilitySink;

    public SinkStore(SinkFunction returnSink, SinkFunction averageSink, SinkFunction volatilitySink) {
        this.returnSink = returnSink;
        this.averageSink = averageSink;
        this.volatilitySink = volatilitySink;
    }

    public SinkFunction getReturnSink() {
        return returnSink;
    }

    public SinkFunction getAverageSink() {
        return averageSink;
    }

    public SinkFunction getVolatilitySink() {
        return volatilitySink;
    }
}
