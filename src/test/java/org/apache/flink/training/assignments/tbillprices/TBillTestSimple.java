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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.training.assignments.domain.TBillRate;
import org.apache.flink.training.assignments.domain.TBillTestResults;
import org.apache.flink.training.assignments.sinks.*;
import org.apache.flink.training.assignments.sources.TBillRateSource;
import org.apache.flink.training.assignments.sources.TestRateSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class TBillTestSimple  {
    private static final Logger LOG = LoggerFactory.getLogger(TBillTestSimple.class);

    //private static final String[] testMonths = new String[]{"2020-03"};
    private static final String[] testMonths = new String[]{"2019-01"};
    //private static final String[] testMonths = new String[]{"2008-01"};


    private static TBillTestResults testResults = null;

    private static SinkStore sinkStore = null;

    @BeforeAll
    public static void readAnswers() {
        testResults = TBillTestResults.ofResource("/tbill_test_data_new.csv");
    }

    @BeforeAll
    public static void setup() {
        sinkStore = new SinkStore(new CollectReturnSink(),
                new CollectAverageSink(),
                new CollectVolatilitySink());

    }

    @Test
    public void testReturns() throws Exception {
        final TBillRateSource source=new TBillRateSource("/TBill_3M_Daily.csv");
        TBillRateStreamingJob job = new TBillRateStreamingJob(testMonths, source, sinkStore);
        job.execute();

        CollectSink sink =  (CollectSink) sinkStore.getReturnSink();

        List<Tuple3<LocalDateTime,Double,Boolean>> actual = sink.getValues();
        assertEquals(testResults.resultMap.get(testMonths[0]).returns.size(), actual.size());
        assertThat( testResults.resultMap.get(testMonths[0]).returns, PriceReturnMacther.matchesPriceReturns(actual));
    }

    @Test
    public void testAverages() throws Exception {
        final TBillRateSource source=new TBillRateSource("/TBill_3M_Daily.csv");
        TBillRateStreamingJob job = new TBillRateStreamingJob(testMonths, source, sinkStore);
        job.execute();

        CollectSink sink =  (CollectSink) sinkStore.getAverageSink();
        List<Tuple3<LocalDateTime,Double,Boolean>> actual = sink.getValues();

        assertThat(actual, hasSize(1));
        assertEquals(true, actual.get(0).f2);
        assertThat(testResults.resultMap.get(testMonths[0]).average, closeTo(actual.get(0).f1,1e-4));
    }

    @Test
    public void testVolatility() throws Exception {
        final TBillRateSource source=new TBillRateSource("/TBill_3M_Daily.csv");
        TBillRateStreamingJob job = new TBillRateStreamingJob(testMonths, source, sinkStore);
        job.execute();

        CollectSink sink =  (CollectSink) sinkStore.getVolatilitySink();
        List<Tuple3<LocalDateTime,Double,Boolean>> actual = sink.getValues();

        assertThat(actual, hasSize(1));
        assertThat(testResults.resultMap.get(testMonths[0]).volatility, closeTo(actual.get(0).f1,1e-3));
    }
}
