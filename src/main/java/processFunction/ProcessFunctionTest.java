package processFunction;


import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
我们知道，DataStream 在调用一些转换方法之后，有可能生成新的流类型；例如调用.keyBy()之后得到 KeyedStream，
进而再调用.window()之后得到 WindowedStream。对于不同类型的流，其实都可以直接用.process()方法进行自定义处理，
这时传入的参数就都叫作处理函数。当然，它们尽管本质相同，都是可以访问状态和时间信息的底层 API，可彼此之间也会
有所差异。
Flink 提供了 8 个不同的处理函数：

（1）ProcessFunction
最基本的处理函数，基于 DataStream 直接调用.process()时作为参数传入。
（2）KeyedProcessFunction
对流按键分区后的处理函数，基于 KeyedStream 调用.process()时作为参数传入。要想使用定时器，比如基于 KeyedStream。
（3）ProcessWindowFunction
开窗之后的处理函数，也是全窗口函数的代表。基于 WindowedStream 调用.process()时作为参数传入。
（4）ProcessAllWindowFunction
同样是开窗之后的处理函数，基于 AllWindowedStream 调用.process()时作为参数传入。
（5）CoProcessFunction
合并（connect）两条流之后的处理函数，基于 ConnectedStreams 调用.process()时作为参数传入。关于流的连接合并操作，我们会在后续章节详细介绍。
（6）ProcessJoinFunction
间隔连接（interval join）两条流之后的处理函数，基于 IntervalJoined 调用.process()时作为参数传入。
（7）BroadcastProcessFunction
广播连接流处理函数，基于 BroadcastConnectedStream 调用.process()时作为参数传入。这里的“广播连接流”BroadcastConnectedStream，
是一个未 keyBy 的普通 DataStream 与一个广播流（BroadcastStream）做连接（conncet）之后的产物。
（8）KeyedBroadcastProcessFunction
按键分区的广播连接流处理函数，同样是基于 BroadcastConnectedStream 调用.process()时作为参数传入。与 BroadcastProcessFunction 不同的是，
这时的广播连接流，是一个 KeyedStream与广播流（BroadcastStream）做连接之后的产物
 */

public class ProcessFunctionTest {
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
                        })
                );
        stream.print();

        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                if(event.user.equals("Mary")){
                    collector.collect(event.user + " clicks " + event.url);
                }else if(event.user.equals("Bob")){
                    collector.collect(event.user);
                    collector.collect(event.user);
                }
                System.out.println("timestamp: " + context.timestamp());
                System.out.println("watermark： " + context.timerService().currentWatermark());

                System.out.println(getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }
        }).print();

        env.execute();
    }
}

