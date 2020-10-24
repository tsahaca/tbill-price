package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.assignments.domain.TBillRate;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonthlyAverage extends KeyedProcessFunction<String, TBillRate, Tuple2<String, Float>> {

    private static final Logger LOG = LoggerFactory.getLogger(MonthlyAverage.class);

    // Keyed, managed state, with an entry for each window, keyed by the window's end time.
    // There is a separate MapState object for each driver.
    //private transient MapState<String, Float> sumOfPrices;
    private transient ValueState<Tuple2<Float, Long>> sumPrice;

    public MonthlyAverage() {

    }

    @Override
    public void open(Configuration conf) throws Exception {
    }

    @Override
    public void processElement(TBillRate value, Context ctx, Collector<Tuple2<String, Float>> out) throws Exception {
        try {
            LOG.debug("inside processElement: {}", value.toString());
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}
