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
import org.apache.flink.training.assignments.domain.TBillRate;
import org.apache.flink.training.assignments.domain.TBillTestResults;
import org.apache.flink.training.assignments.sinks.*;
import org.apache.flink.training.assignments.sources.TestRateSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;



public class TBillPriceTestNew extends TestBase<TBillRate,Tuple3<LocalDateTime, Double,Boolean>> {
    private static final Logger LOG = LoggerFactory.getLogger(TBillPriceTestNew.class);

    private static final String[] testMonths = new String[]{"2020-03"};

    private static final Testable TEST_APPLICATION = () -> TBillPriceAssignmentNew.main(testMonths);
    private static TBillTestResults testResults = null;

    @BeforeAll
    public static void readAnswers() {
        testResults = TBillTestResults.ofResource("/tbill_test_data_new.csv");
    }



    @Test
    public void testReturns() throws Exception {
        TestRateSource source = new TestRateSource();

        List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source,SinkContainer.RETURN_SINK);
        assertEquals(testResults.resultMap.get(testMonths[0]).returns.size(), actual.size());
        assertThat( testResults.resultMap.get(testMonths[0]).returns, PriceReturnMacther.matchesPriceReturns(actual));

    }

    @Test
    public void testAverages() throws Exception {

        TestRateSource source = new TestRateSource();
        List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source, SinkContainer.AVERAGE_SINK);
        //List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source);

        assertThat(actual, hasSize(1));
        assertEquals(true, actual.get(0).f2);
        assertThat(testResults.resultMap.get(testMonths[0]).average, closeTo(actual.get(0).f1,1e-4));

    }

    @Test
    public void testVolatility() throws Exception {
        TestRateSource source = new TestRateSource();
        List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source, SinkContainer.VOLATILITY_SINK);
        assertThat(actual, hasSize(1));
        assertThat(testResults.resultMap.get(testMonths[0]).volatility, closeTo(actual.get(0).f1,1e-1));

    }

    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source, SinkFunction sink) throws Exception {

            return runApp(source, sink, TEST_APPLICATION);
    }

}
