package windowAndTime;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream= env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness( Duration.ZERO )
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

// todo
        stream.map(new MapFunction<Event, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user,1L);
            }
        })
                        .keyBy(data -> data.f0)
                //                .countWindow(10,2) // 滑动技术窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2))) // 事件时间会话窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5))) // 滑动事件事件窗口

                                // 滚动事件事件窗口  todo   打开一个五秒的窗口
                                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                                            @Override
                                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                                            }
                                        })
                                                .print();

        env.execute();

    }

}

