package windowAndTime;

import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateDataTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);


//        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
//                // 乱序流的watermark生成
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("slave01",7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        String[] fields = s.split(",");

                        return new Event(fields[0].trim(),fields[1].trim(),Long.valueOf(fields[2].trim()) );
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.print("data");
        // 定义一个输出标签
        //  会涉及到泛型擦除
        OutputTag<Event> late = new OutputTag<Event>("late"){};

        // 统计每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());
        result.print("result");
        result.getSideOutput(late).print("late");
        env.execute();
    }
}

