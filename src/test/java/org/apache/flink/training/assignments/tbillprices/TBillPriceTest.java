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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.time.LocalDateTime;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import static  org.hamcrest.MatcherAssert.assertThat;


public class TBillPriceTest extends TestBase<TBillRate,Tuple3<LocalDateTime, Double,Boolean>> {
    private static final Logger LOG = LoggerFactory.getLogger(TBillPriceTest.class);

    private static final String[] testMonths = new String[]{"2020-03"};

    private static final Testable TEST_APPLICATION = () -> TBillPriceAssignment.main(testMonths);
    private static TBillTestResults testResults = null;

    @BeforeAll
    public static void readAnswers() {
        testResults = TBillTestResults.ofResource("/tbill_test_data_new.csv");
    }
    /**
    @Test
    public void testReturns() throws Exception {
        TestRateSource source = new TestRateSource();
        List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source,new PriceSink());
        //List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source);
        assertEquals(testResults.resultMap.get(testMonths[0]).returns.size(), actual.size());
        assertThat( testResults.resultMap.get(testMonths[0]).returns, PriceReturnMacther.matchesPriceReturns(actual));
    }

    @Test
    public void testPriceMatcher() throws Exception{
        //2020-03-03T00:00,5.499547944442107E-4,false
        Tuple3<LocalDateTime, Double, Boolean> tuple1=Tuple3.of(LocalDateTime.parse("2020-03-03T00:00"),5.499547944442107e-4,false);
        //2020-03-04T00:00,6.154892300197767E-4,false
        Tuple3<LocalDateTime, Double, Boolean> tuple2=Tuple3.of(LocalDateTime.parse("2020-03-04T00:00"),6.154892300197767E-4,false);
        //2020-03-05T00:00,2.025473395252276E-4,false
        Tuple3<LocalDateTime, Double, Boolean> tuple3=Tuple3.of(LocalDateTime.parse("2020-03-05T00:00"),2.025473395252276E-4,false);


        //2020-03-03T00:00,5.49955E-4,false
        Tuple3<LocalDateTime, Double, Boolean> tuple11=Tuple3.of(LocalDateTime.parse("2020-03-03T00:00"),5.49955e-4,false);
        //2020-03-04T00:00,6.15489E-4,false
        Tuple3<LocalDateTime, Double, Boolean> tuple22=Tuple3.of(LocalDateTime.parse("2020-03-04T00:00"),6.15489E-4,false);
        //2020-03-05T00:00,2.02547E-4,false
        Tuple3<LocalDateTime, Double, Boolean> tuple33=Tuple3.of(LocalDateTime.parse("2020-03-05T00:00"),2.02547E-4,false);

        List<Tuple3<LocalDateTime, Double, Boolean>> actual = Arrays.asList(tuple1,tuple2,tuple3);
        List<Tuple3<LocalDateTime, Double, Boolean>> expected = Arrays.asList(tuple11,tuple22,tuple33);
        assertThat(expected, PriceReturnMacther.matchesPriceReturns(actual));

    }

    @Test
    public void testAverages() throws Exception {

        TestRateSource source = new TestRateSource();
        List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source, new AverageSink());
        //List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source);

        assertThat(actual, hasSize(1));
        assertEquals(true, actual.get(0).f2);
        assertThat(testResults.resultMap.get(testMonths[0]).average, closeTo(actual.get(0).f1,1e-4));

    }

    @Test
    public void testVolatility() throws Exception {
        TestRateSource source = new TestRateSource();
        List<Tuple3<LocalDateTime,Double,Boolean>> actual = results(source, new VolatilitySink());
        assertThat(actual, hasSize(1));
        assertThat(testResults.resultMap.get(testMonths[0]).volatility, closeTo(actual.get(0).f1,1e-2));

    }

    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source) throws Exception {
        return runApp(source, new TestSink<>(), TEST_APPLICATION);
    }

    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source, PriceSink sink) throws Exception {
        return runApp(source, sink, TEST_APPLICATION);
    }

    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source, AverageSink sink) throws Exception {
        return runApp(source, sink, TEST_APPLICATION);
    }

    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source, VolatilitySink sink) throws Exception {
        return runApp(source, sink, TEST_APPLICATION);
    }
    */

}
