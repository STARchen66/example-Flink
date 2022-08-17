package windowAndTime;

import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 2000L),
                new Event("BOb", "./prod?id=100", 3000L),
                new Event("BOb", "./prod?id=18", 3900L),
                new Event("BOb", "./prod?id=140", 4000L),
                new Event("Alice", "./prod?id=140", 5000L),
                new Event("Alice", "./prod?id=240", 20000L),
                new Event("Alice", "./prod?id=040", 5000L))
                // 有序流的watermark生成  todo forMonotonousTimestamps
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                return element.timestamp;
//                            }
//                        }));
        //乱序流的watermark生成  todo  forBoundedOutOfOrderness
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
    }
}
