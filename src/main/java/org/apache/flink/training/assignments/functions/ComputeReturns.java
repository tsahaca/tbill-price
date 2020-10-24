package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.PriceReturns;
import org.apache.flink.training.assignments.domain.TBillRate;
import org.apache.flink.util.Collector;

// TODO: enforce a window size of 2...
public class ComputeReturns extends ProcessWindowFunction<TBillRate, PriceReturns, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<TBillRate> rates, Collector<PriceReturns> out) {

    }
}
