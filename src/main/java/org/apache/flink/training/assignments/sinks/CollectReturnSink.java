package org.apache.flink.training.assignments.sinks;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class CollectReturnSink<OUT> implements CollectSink, SinkFunction<OUT> {
    // must be static
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
