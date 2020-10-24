package org.apache.flink.training.assignments.domain;

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class TBillTestRecord {
    public List<Tuple3<LocalDateTime, Double, Boolean>> returns = new ArrayList<>();
    public Double average;
    public Double volatility;
}
