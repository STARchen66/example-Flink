package streamConversion;


import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
不要把窗口时间确定死，基于数据后面一段时间的触发而已

间隔联结的思路就是针对一条流的每个数据，开辟出其时间戳前后的一段时间间隔，看这期间是否有来自另一条流的数据匹配。

todo 1. 间隔联结的原理
间隔联结具体的定义方式是，我们给定两个时间点，分别叫作间隔的“上界”（upperBound）和“下界”（lowerBound）；
于是对于一条流（不妨叫作 A）中的任意一个数据元素 a，就可以开辟一段时间间隔：[a.timestamp + lowerBound, a.timestamp + upperBound],
即以 a 的时间戳为中心，下至下界点、上至上界点的一个闭区间：我们就把这段时间作为可以匹配另一条流数据的“窗口”范围。
所以对于另一条流（不妨叫 B）中的数据元素 b，如果它的时间戳落在了这个区间范围内，a 和 b 就可以成功配对，进而进行计算输出结果。所以匹配的条件为：
todo  a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound
这里需要注意，做间隔联结的两条流 A 和 B，也必须基于相同的 key；下界 lowerBound应该小于等于上界 upperBound，两者都可正可负；间隔联结目前只支持事件时间语义。

todo 间隔联结在代码中，是基于 KeyedStream 的联结（join）操作。
DataStream 在 keyBy 得到KeyedStream 之后，可以调用.intervalJoin()来合并两条流，传入的参数同样是一个 KeyedStream，
两者的 key 类型应该一致；得到的是一个 IntervalJoin 类型。后续的操作同样是完全固定的：
先通过.between()方法指定间隔的上下界，再调用.process()方法，定义对匹配数据对的处理操作。
调用.process()需要传入一个处理函数，这是处理函数家族的最后一员：“处理联结函数”ProcessJoinFunction。

stream1
 .keyBy(<KeySelector>)
 .intervalJoin(stream2.keyBy(<KeySelector>))
 .between(Time.milliseconds(-2), Time.milliseconds(1))
 .process (new ProcessJoinFunction<Integer, Integer, String(){
 @Override
 public void processElement(Integer left, Integer right, Context ctx,
Collector<String> out) {
 out.collect(left + "," + right);
 }
 });

 在电商网站中，某些用户行为往往会有短时间内的强关联。我们这里举一个例子，我们有两条流，一条是下订单的流，一条是浏览数据的流。
 我们可以针对同一个用户，来做这样一个联结。也就是使用一个用户的下订单的事件和这个用户的最近十分钟的浏览数据进行一个联结查询
 */


public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                Tuple2.of("Mary", 5000L),
                Tuple2.of("Alice", 5000L),
                Tuple2.of("Bob", 20000L),
                Tuple2.of("Alice", 20000L),
                Tuple2.of("Alice", 51000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                        return stringLongTuple2.f1;
                    }
                })
        );

        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 2000L),
                new Event("BOb", "./prod?id=100", 3000L),
                new Event("BOb", "./prod?id=18", 3900L),
                new Event("BOb", "./prod?id=140", 4000L),
                new Event("Alice", "./prod?id=140", 5000L),
                new Event("Alice", "./prod?id=240", 20000L),
                new Event("Alice", "./prod?id=040", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        );

        orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(data -> data.user))
                .between(Time.seconds(-5),Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Event event, ProcessJoinFunction<Tuple2<String, Long>, Event, Object>.Context context, Collector<Object> collector) throws Exception {
                        collector.collect(event + " =》 " + stringLongTuple2);
                    }
                }).print();


        env.execute();
    }
}


