package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.training.assignments.domain.TBillRate;

public class AverageAggregate
        implements AggregateFunction<TBillRate, Tuple2<Double, Long>, Double> {

    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return new Tuple2<>(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(TBillRate value, Tuple2<Double, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 + value.getClosingPrice(), accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> accumulator) {
        return (accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}