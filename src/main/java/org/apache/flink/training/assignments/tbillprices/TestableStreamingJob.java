package org.apache.flink.training.assignments.tbillprices;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TestableStreamingJob {
    private SourceFunction<Long> source;
    private SinkFunction<Long> sink;

    public TestableStreamingJob(SourceFunction<Long> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> LongStream =
                env.addSource(source)
                        .returns(TypeInformation.of(Long.class));

        LongStream
                .map(new IncrementMapFunction())
                .addSink(sink);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        TestableStreamingJob job = new TestableStreamingJob(new RandomLongSource(), new PrintSinkFunction<>());
        job.execute();
    }

    // While it's tempting for something this simple, avoid using anonymous classes or lambdas
    // for any business logic you might want to unit test.
    public class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(Long record) throws Exception {
            return record + 1 ;
        }
    }

}