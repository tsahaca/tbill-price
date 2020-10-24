package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.training.assignments.domain.MonthlyAverageReturn;
import org.apache.flink.training.assignments.domain.PriceReturns;
import org.apache.flink.training.assignments.tbillprices.TBillPriceAssignment;
import org.apache.flink.util.Collector;
import java.util.Map;

import java.time.format.DateTimeFormatter;

public class MonthlyWindowAverage extends RichFlatMapFunction<PriceReturns, MonthlyAverageReturn> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Float>> sum;
    private MapState<String,Float> mavReturnMap;

    @Override
    public void flatMap(PriceReturns input, Collector<MonthlyAverageReturn> out) throws Exception {
        // access the state value
        Tuple2<Long, Float> currentSum = sum.value();
        currentSum.f0 += 1;
        currentSum.f1 += input.getAmount();
        sum.update(currentSum);

        // if endOfMonth, calculate, collect and clear the ValueState
        if (input.isEndOfMonth()) {
            MonthlyAverageReturn mAvg = new MonthlyAverageReturn( input.getDate().format(DateTimeFormatter.ofPattern("yyyy-MM")),
                    sum.value().f1/21);
            out.collect(mAvg);

            mavReturnMap.put( input.getDate().format(DateTimeFormatter.ofPattern("yyyy-MM")),
                    sum.value().f1);
            sum.clear();
        }

    }

    @Override
    public void open(Configuration config) {

        ValueStateDescriptor<Tuple2<Long, Float>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Float>>() {}), // type information
                        Tuple2.of(0L, 0F)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);

        MapStateDescriptor<String,Float> mapStateDescriptor = new MapStateDescriptor<String, Float>("MonthlyAvgReturnMapState",
                String.class,Float.class);

        mavReturnMap = getRuntimeContext().getMapState(mapStateDescriptor);


    }
}