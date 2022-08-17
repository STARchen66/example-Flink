package windowAndTime;

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

//todo  把每个url页面的访问量按照窗口划分

public class UrlCountViewExample {
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
        // 统计每个url的访问量
        stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();
        env.execute();
    }
    // 增量聚合，来一条数据就加1
    public static class UrlViewCountAgg implements AggregateFunction<Event,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
    // 包装窗口信息， 输出UrlViewCount
    public static class UrlViewCountResult extends ProcessWindowFunction<Long,UrlViewCount,String,TimeWindow>{
        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(new UrlViewCount(s,count,start,end));
        }
    }
}

