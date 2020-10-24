package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.*;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.assignments.domain.MonthlyAverageReturn;
import org.apache.flink.training.assignments.domain.PriceReturns;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;


public class ComputeVolatility extends RichCoFlatMapFunction<PriceReturns, MonthlyAverageReturn, Tuple3<String, Double,Boolean>> {
    // keyed, managed state
    private ListState<PriceReturns> priceReturnsListState;
    private ValueState<MonthlyAverageReturn> monthlyAverageReturnValueState;
    private static final Logger LOG = LoggerFactory.getLogger(ComputeVolatility.class);


    @Override
    public void open(Configuration config) {
        priceReturnsListState = getRuntimeContext().getListState(new ListStateDescriptor<>("Saved Daily Price", PriceReturns.class));
        monthlyAverageReturnValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("Saved Monthly Average", MonthlyAverageReturn.class));

    }

    @Override
    public void flatMap1(PriceReturns value, Collector<Tuple3<String, Double,Boolean>> out) throws Exception {
        // access the state value
        priceReturnsListState.add(value);
    }

    @Override
    public void flatMap2(MonthlyAverageReturn value, Collector<Tuple3<String,Double,Boolean>> out) throws Exception {
        monthlyAverageReturnValueState.update(value);
        Float totalSum=0F;
        Float mavg = value.getAmount();
        long counter=0;
        PriceReturns temp=null;
        Float sum=0F;
        for (PriceReturns item: priceReturnsListState.get()) {
            ++counter;
            Float S = (item.getAmount() - mavg) * (item.getAmount() - mavg);
            totalSum +=S;

            temp=item;
        }
        if( counter > 1 && temp.isEndOfMonth()) {
            //Double volatility = (Math.sqrt(totalSum / (counter-1)) * Math.sqrt(252))*100;
            Double volatility = (Math.sqrt(totalSum / 20) * Math.sqrt(252))*100;
            Tuple3<String,Double,Boolean> ret = Tuple3.of(temp.getDate().format(DateTimeFormatter.ofPattern("yyyy-MM")),
                    volatility,temp.isEndOfMonth() );
            //LOG.info(ret.toString());
            //LOG.info("Month: {} , Volatilty: {}", ret.f0, ret.f1);
            out.collect(ret);
            priceReturnsListState.clear();
        }

    }
}