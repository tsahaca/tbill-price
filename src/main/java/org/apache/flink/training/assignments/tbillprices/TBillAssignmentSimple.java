package org.apache.flink.training.assignments.tbillprices;


import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.training.assignments.sinks.SinkStore;
import org.apache.flink.training.assignments.sources.TBillRateSource;

public class TBillAssignmentSimple {

    public static void main(String[] args) throws Exception {

        final  TBillRateSource source=new TBillRateSource("/TBill_3M_Daily.csv");

        final SinkStore sinkStore = new SinkStore(new PrintSinkFunction(),
                new PrintSinkFunction(),
                new PrintSinkFunction());

        TBillRateStreamingJob job = new TBillRateStreamingJob(args, source, sinkStore);
        job.execute();
    }
}