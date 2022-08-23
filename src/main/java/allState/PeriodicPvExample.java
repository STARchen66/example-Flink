package allState;


import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
1. 值状态（ValueState）
我们这里会使用用户 id 来进行分流，然后分别统计每个用户的 pv 数据，
由于我们并不想每次 pv 加一，就将统计结果发送到下游去，所以这里我们注册了一个定时器，用来隔一段时间发送 pv 的统计结果，
这样对下游算子的压力不至于太大。具体实现方式是
todo 定义一个用来保存定时器时间戳的值状态变量。当定时器触发并向下游发送数据以后，便清空储存定时器时间戳的状态变量，这样当新的数据到来时，发现并没有定时器存在，就可以注册新的定时器了,
注册完定时器之后将定时器的时间戳继续保存在状态变量中。
 */

public class PeriodicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.print("input");
        // 统计每个用户的pv
        stream.keyBy(data -> data.user)
                .process(new PeriodicPvPesult())
                .print();

        env.execute();
    }
    // 实现自定义的KeyedProcessFunction
    public static class PeriodicPvPesult extends KeyedProcessFunction<String, Event, String>{
        // 定义状态，保存当前pv统计值， 以及有没有定时器
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，就更新对应的count值
            Long count = countState.value();
            countState.update(count  == null ? 1 : count + 1);
            // 注册定时器，如果没有注册定时器的话，采取注册定时器
            if(timerTsState.value() == null){
                context.timerService().registerEventTimeTimer(event.timestamp + 10 * 1000L);
                timerTsState.update(event.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发输出一次统计结果
            out.collect(ctx.getCurrentKey() + " pv: " + countState.value());
            // 清空状态
            timerTsState.clear();
            //countState.clear();   如果加上就和十秒钟滚动窗口类似了，但又不完全一样， 我们是基于每来一条数据之后才去定义定时器

            // 不增加，数据到来时才触发注册定时器
            // 增加了，就立马注册定时器
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerTsState.update(timestamp + 10 * 1000L);
        }
    }
}

