package org.apache.flink.training.assignments.assigners;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.TBillRate;

public class WatermarkOnFlagAssigner implements AssignerWithPunctuatedWatermarks<TBillRate> {

    public long extractTimestamp(TBillRate element, long previousElementTimestamp) {
        return element.getEventTime();
    }

    public Watermark checkAndGetNextWatermark(TBillRate lastElement, long extractedTimestamp) {
        return lastElement.isEndOfMonth() ? new Watermark(lastElement.getEventTime()) : null;
    }
}
