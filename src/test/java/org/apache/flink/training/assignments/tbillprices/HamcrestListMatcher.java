package org.apache.flink.training.assignments.tbillprices;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Every.everyItem;

import org.apache.flink.shaded.guava18.com.google.common.math.DoubleMath;
import org.junit.jupiter.api.Test;
import org.hamcrest.Matcher;
import org.apache.flink.api.java.tuple.Tuple3;




public class HamcrestListMatcher {

    @Test
    public void testList() {
        List<Integer> list = Arrays.asList(5, 2, 4);

        assertThat(list, hasSize(3));

        // ensure the order is correct
        assertThat(list, contains(5, 2, 4));

        assertThat(list, containsInAnyOrder(2, 4, 5));

        assertThat(list, everyItem(greaterThan(1)));

        Double[] doubles = {5.499547944442107E-4, 2.35556, 4.9888};
       // List<Double> expected =  Arrays.asList(5.44498, 2.355568, 4.98889);
        double[] expected =  {5.49955E-4, 2.355568, 4.98889};

        DoubleMath.fuzzyEquals(5.499547944442107E-4,5.49955E-4,1e-4);

        //assertThat(doubles, contains(expected.toArray()));
        assertThat(doubles, arrayCloseTo(expected,1e-4));

    }

    public static Matcher<Double[]> arrayCloseTo(double[] array, double error) {
        List<Matcher<? super Double>> matchers = new ArrayList<Matcher<? super Double>>();
        for (double d : array)
            matchers.add(closeTo(d, error));
        return arrayContaining(matchers);
    }



    @Test
    public void arrayContainsNumbersInGivenOrder() {
        Integer[] ints = new Integer[] { 7, 5, 12, 16 };

        assertThat(ints, arrayContaining(7, 5, 12, 16));
    }


}
