package windowAndTime;


import com.typesafe.sslconfig.ssl.FakeChainedKeyStore;
import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;


//todo  3. 增量聚合和全窗口函数的结合使用

public class UvCountExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.print("data");
        // 使用AggregateFunction 和 ProcessWindowFunction结合计算UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UvAgg(), new UvCountResult())
                .print();

        env.execute();
    }
    // 自定义实现AggregateFunction，增量聚合计算NV值
    public static class UvAgg implements AggregateFunction<Event,HashSet<String>,Long>{
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.user);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }



    // 实现自定义的ProcessWindowFunction ，包装窗口信息
    public static class UvCountResult extends ProcessWindowFunction<Long,String,Boolean,TimeWindow>{
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long uv = elements.iterator().next();
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(("窗口： " + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " UV值为： " + uv));
        }
    }
}


