package streamConversion;


import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
最简单的合流操作，就是直接将多条流合在一起，叫作流的“联合”（union）
联合操作要求必须流中的数据类型必须相同，合并之后的新流会包括所有流中的元素，数据类型不变。这种合流方式非常简单粗暴，就像公路上多个车道汇在一起一样。
在代码中，我们只要基于 DataStream 直接调用.union()方法，传入其他 DataStream 作为参数，就可以实现流的联合了；得到的依然是一个 DataStream：

stream1.union(stream2, stream3, ...)
1
注意：union()的参数可以是多个 DataStream，所以联合操作可以实现多条流的合并。
 */


public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("hadoop102",7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(),field[1].trim(), Long.valueOf(field[2].trim()) );
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream1.print("stream1");
        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("hadoop103",7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(),field[1].trim(), Long.valueOf(field[2].trim()) );
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream2.print("stream2");
        // 合并两条流
        stream1.union(stream2).process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect("水位线： " + context.timerService().currentWatermark());
            }

        }).print();
        env.execute();
    }

}

