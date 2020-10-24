package org.apache.flink.training.assignments.sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class CollectVolatilitySink<OUT> implements CollectSink, SinkFunction<OUT> {
    public  static final List VALUES = new ArrayList<>();
    @Override
    public void invoke(OUT value, Context context) {
        VALUES.add(value);
    }

    public List getValues(){
        return VALUES;
    }

    public void clearValues(){
        VALUES.clear();
    }

}
