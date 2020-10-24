package org.apache.flink.training.assignments.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.training.assignments.sources.TestSource;

public class TestStringSource extends TestSource<String> implements ResultTypeQueryable<String> {
    public TestStringSource(Object... eventsOrWatermarks) {
        this.testStream = eventsOrWatermarks;
    }

    @Override
    long getTimestamp(String s) {
        return 0L;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
