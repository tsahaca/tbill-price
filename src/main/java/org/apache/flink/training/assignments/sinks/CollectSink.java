package org.apache.flink.training.assignments.sinks;

import java.util.List;

public interface CollectSink  {
    List getValues();
    void clearValues();
}
