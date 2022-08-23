package allState;


import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;

/*
3. 映射状态（MapState）
映射状态的用法和 Java 中的 HashMap 很相似。在这里我们可以通过 MapState 的使用来探索一下窗口的底层实现，
也就是我们要用映射状态来完整模拟窗口的功能。这里我们模拟一个滚动窗口。我们要计算的是每一个 url 在每一个窗口中的 pv 数据。
我们之前使用增量聚合和全窗口聚合结合的方式实现过这个需求。这里我们用 MapState 再来实现一下。

每一个窗口都应该有个list，来保存当前的所有数据。不同窗口的数据还应该按照窗口的起始或者结束时间戳去做分别的保存。

（已经按照不同的key划分开了）我们可以使用HashMap， key为窗口的id，value为count值
 */

public class FakedWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        stream.keyBy(data-> data.url)
                .process(new FakedWindowResult(10000L))
                .print();
        env .execute();
    }


    // 实现自定义的KeyedProcessFunction
    public static class FakedWindowResult extends KeyedProcessFunction<String, Event, String> {
        private Long windowSize;

        public FakedWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 定义一个mapState，用来保存每个窗口中统计的count值
        MapState<Long, Long> windowUrlCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-Count", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，根据数据戳判断属于哪个窗口（窗口分配器）
            Long windowStart = event.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;
            // 注册 end - 1 的定时器
            context.timerService().registerEventTimeTimer(windowEnd - 1);
            // 更新状态，进行增量聚合
            if (windowUrlCountMapState.contains(windowStart)) {
                Long count = windowUrlCountMapState.get(windowStart);
                windowUrlCountMapState.put(windowStart, count + 1);
            } else {
                windowUrlCountMapState.put(windowStart, 1L);
            }
        }


        // 定时器触发时 输出计算结果
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = windowUrlCountMapState.get(windowStart);
            out.collect("窗口： " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                    + " Url: " + ctx.getCurrentKey()
                    + " count : " + count);
            // 模拟窗口的关闭，清除 map中对应的key-value
            windowUrlCountMapState.remove(windowStart);
        }
    }
}


