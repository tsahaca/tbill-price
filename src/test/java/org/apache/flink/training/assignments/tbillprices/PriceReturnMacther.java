package org.apache.flink.training.assignments.tbillprices;



import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.guava18.com.google.common.math.DoubleMath;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import javax.swing.text.StyledEditorKit;
import java.time.LocalDateTime;
import java.util.List;

public class PriceReturnMacther extends TypeSafeMatcher<List<Tuple3<LocalDateTime, Double,Boolean>>> {



    private final List<Tuple3<LocalDateTime, Double,Boolean>> expected;

    public PriceReturnMacther(final List<Tuple3<LocalDateTime, Double, Boolean>> expected) {
        this.expected = expected;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("matches price return");
    }

    @Override
    public boolean matchesSafely(final List<Tuple3<LocalDateTime, Double,Boolean>> actual) {
        int expectedCount=this.expected.size();
        int actualCount=actual.size();
        int runningCount=0;
        for(Tuple3<LocalDateTime, Double,Boolean> item: actual){
            Tuple3<LocalDateTime, Double,Boolean> expectedItem = this.expected.get(runningCount);

            if(item.f0.equals(expectedItem.f0) &&
                    DoubleMath.fuzzyEquals(item.f1,expectedItem.f1,1e-4) &&
                    item.f2.equals(expectedItem.f2)) {
                ++runningCount;

            }


        }
        return actualCount == runningCount;

    }


    // matcher method you can call on this matcher class
    public static PriceReturnMacther matchesPriceReturns(final  List<Tuple3<LocalDateTime, Double,Boolean>> expected) {
        return new PriceReturnMacther(expected);
    }
}