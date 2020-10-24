/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.assignments.tbillprices;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.training.assignments.sinks.*;
import org.apache.flink.training.assignments.domain.MonthlyAverageReturn;
import org.apache.flink.training.assignments.domain.PriceReturns;
import org.apache.flink.training.assignments.domain.TBillRate;
import org.apache.flink.training.assignments.functions.ComputeVolatility;
import org.apache.flink.training.assignments.functions.MonthlyWindowAverage;
import org.apache.flink.training.assignments.functions.Tail;
import org.apache.flink.training.assignments.sources.TBillRateSource;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class TBillPriceAssignmentNew extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(TBillPriceAssignmentNew.class);

    public static String getKey(LocalDateTime dt) {
        return dt.format(DateTimeFormatter.ofPattern("yyyy-MM"));
    }

    public static boolean isIn(String value, String... keys) {
        return new HashSet<>(Arrays.asList(keys)).contains(value);
    }
   

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(ExerciseBase.parallelism);
        env.setParallelism(1);



        // read in the rates and group by year and month: e.g. 2020-04
        DataStream<TBillRate> rates = env.addSource(new TBillRateSource("/TBill_3M_Daily.csv"))
                .keyBy(TBillRate::getKey);
        DataStream<Tuple3<LocalDateTime, Double, Boolean>> prices = getPrices(args, rates);

        DataStream<Tuple3<LocalDateTime, Double, Boolean>> returns = computeReturns(prices);
        returns.addSink(SinkContainer.RETURN_SINK);

        DataStream<Tuple3<LocalDateTime, Double, Boolean>> avgReturn = computeAverages(returns);
        avgReturn.addSink(SinkContainer.AVERAGE_SINK);


        avgReturn.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "**** Element {}"));

        final DataStream<PriceReturns> dailyReturns = returns.map( item -> new PriceReturns(item.f0, new Float(item.f1), item.f2))
                                                              .keyBy((PriceReturns item) -> getKey(item.getDate()));
        final DataStream<MonthlyAverageReturn> monthlyAverageReturns = computeMonthlyAverage(dailyReturns)
                                                                       .keyBy((MonthlyAverageReturn item) -> item.getDate());

        //printOrTest(monthlyAverageReturns);
        final DataStream<Tuple3<String, Double,Boolean>> volatility = computeAnnualizedVolatility(dailyReturns,monthlyAverageReturns);
        volatility.addSink(SinkContainer.VOLATILITY_SINK);





        // execute the transformation pipeline
        env.execute("TBillPrice");
    }

    public static DataStream<Tuple3<LocalDateTime, Double, Boolean>> computeAverages(DataStream<Tuple3<LocalDateTime, Double, Boolean>> returns) {
        // Calculate a running average return for each month, return only the last element in the month (boolean flag is true)
        return (DataStream<Tuple3<LocalDateTime, Double, Boolean>>) returns
                .map(t -> Tuple4.of(t.f0, t.f1, 1l, t.f2))
                .returns(Types.TUPLE(Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.LONG, Types.BOOLEAN))
                .keyBy(t -> getKey(t.f0))
                .reduce((t0, t1) -> Tuple4.of(t0.f0, t0.f1 + t1.f1, t0.f2 + t1.f2, t1.f3))
                //.map(t -> Tuple3.of(getKey(t.f0), t.f1 / t.f2, t.f3))
                .map(t -> Tuple3.of(t.f0, t.f1 / t.f2, t.f3))
                .returns(Types.TUPLE(Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.BOOLEAN))
                .filter(t -> t.f2);
    }

    public static DataStream<Tuple3<LocalDateTime, Double, Boolean>> computeReturns(DataStream<Tuple3<LocalDateTime, Double, Boolean>> prices) {
        // Get a stream of price returns
        // Since we are dealing with streams, as a work-around for reduce remove first element from each group
        return (DataStream<Tuple3<LocalDateTime, Double, Boolean>>) prices
                .keyBy(t -> getKey(t.f0))
                .countWindow(2, 1)
                .reduce((t0, t1) -> Tuple3.of(t1.f0, Math.log(t1.f1 / t0.f1), t1.f2))
                .keyBy(t -> getKey(t.f0))
                .process(new Tail())
                .returns(Types.TUPLE(Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.BOOLEAN));

    }

    public static DataStream<Tuple3<LocalDateTime, Double, Boolean>> getPrices(String[] args, DataStream<TBillRate> rates) {
        // For unit testing, pass in args array such as: ["2008-01","2020-03"] etc
        return (args != null && args.length > 0) ?
                rates
                        .map(r -> Tuple3.of(r.getQuoteTime(), r.getClosingPrice(), r.isEndOfMonth()))
                        .returns(Types.TUPLE(Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.BOOLEAN))
                        .filter(t -> isIn(getKey(t.f0), args))
                :
                rates
                        .map(r -> Tuple3.of(r.getQuoteTime(), r.getClosingPrice(), r.isEndOfMonth()))
                        .returns(Types.TUPLE(Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.BOOLEAN));
    }

    public static DataStream<Tuple3<String, Double,Boolean>> computeAnnualizedVolatility(final DataStream<PriceReturns> dailyReturnsByKey,
                                                                      final DataStream<MonthlyAverageReturn> averageMonthlyReturnsByKey){

        DataStream<Tuple3<String, Double,Boolean>> annualizedMonthlyVolatility = dailyReturnsByKey
                .connect(averageMonthlyReturnsByKey)
                .flatMap(new ComputeVolatility())
                .uid("volatility");
        return annualizedMonthlyVolatility;

    }

    public static  DataStream<MonthlyAverageReturn> computeMonthlyAverage(final DataStream<PriceReturns> dailyReturns ){
        DataStream<MonthlyAverageReturn>  averageMonthlyReturns = dailyReturns
                .keyBy(item -> getKey(item.getDate()))
                .flatMap(new MonthlyWindowAverage());
        return averageMonthlyReturns;
    }

}
