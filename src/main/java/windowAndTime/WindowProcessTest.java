package windowAndTime;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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


/*
（2）处理窗口函数（ProcessWindowFunction）
ProcessWindowFunction 是 Window API 中最底层的通用窗口函数接口。
之所以说它“最底层”，是因为除了可以拿到窗口中的所有数据之外，ProcessWindowFunction 还可以获取到一个“上下文对象”（Context）。
这个上下文对象非常强大，不仅能够获取窗口信息，还可以访问当前的时间和状态信息。
这里的时间就包括了处理时间（processing time）和事件时间水位线（event time watermark）。
这就使得 ProcessWindowFunction 更加灵活、功能更加丰富。
 */

public class WindowProcessTest {
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
        // 使用ProcessWindowFunction计算UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print();

        env.execute();
    }

    // 实现自定义的ProcessWIndowFunction ，输出一条统计信息
    public static class UvCountByWindow extends ProcessWindowFunction<Event,String,Boolean,TimeWindow>{
        @Override
        public void process(Boolean aBoolean,
                            ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context,
                            Iterable<Event> elements,
                            Collector<String> out) throws Exception {
            //用一个HashSet保存User
             HashSet<String> userSet = new HashSet<>();
             //从elements中遍历数据，放到set中去重
            for (Event element : elements) {
                userSet.add(element.user);
            }
            Integer uv = userSet.size();
            //结合窗口信息
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(("窗口： " + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " UV值为： " + uv));
        }
    }
}

