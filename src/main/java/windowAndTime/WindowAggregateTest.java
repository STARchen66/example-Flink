package windowAndTime;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

/*
（2）聚合函数（AggregateFunction）
ReduceFunction 可以解决大多数归约聚合的问题，但是这个接口有一个限制，就是聚合状态的类型、输出结果的类型都必须和输入数据类型一样。
这就迫使我们必须在聚合前，先将数据转换（map）成预期结果类型；而在有些情况下，还需要对状态进行进一步处理才能得到输出结果，
这时它们的类型可能不同，使用 ReduceFunction 就会非常麻烦。

AggregateFunction 可以看作是 ReduceFunction 的通用版本，
这里有三种类型：输入类型（IN）、累加器类型（ACC）和输出类型（OUT）。
输入类型 IN 就是输入流中元素的数据类型；累加器类型 ACC 则是我们进行聚合的中间状态类型；而输出类型当然就是最终计算结果的类型了。

接口中有四个方法：

createAccumulator()：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。

add()：              将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进一步聚合的过程。
                     方法传入两个参数：当前新到的数据 value，和当前的累加器accumulator；返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之后都会调用这个方法。

getResult()：        从累加器中提取聚合的输出结果。也就是说，我们可以定义多个状态，然后再基于这些聚合的状态计算出一个结果进行输出。
                     比如之前我们提到的计算平均值，就可以把 sum 和 count 作为状态放入累加器，而在调用这个方法时相除得到最终结果。这个方法只在窗口要输出结果时调用。

merge()：            合并两个累加器，并将合并后的状态作为一个累加器返回。
                     这个方法只在需要合并窗口的场景下才会被调用；最常见的合并窗口（Merging Window）的场景就是会话窗口（Session Windows）。
 */

public class WindowAggregateTest {
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

        stream.keyBy(data -> data.user)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                .aggregate(new AggregateFunction<Event, Tuple2<Long,Integer>, String>() {
                                    @Override
                                    public Tuple2<Long, Integer> createAccumulator() {
                                        return Tuple2.of(0L,0);
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0+value.timestamp,accumulator.f1+1);
                                    }

                                    @Override
                                    public String getResult(Tuple2<Long, Integer> accumulator) {
                                        Timestamp timestamp = new Timestamp(accumulator.f0 / accumulator.f1);
                                        return timestamp.toString();
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                                        return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                                    }
                                })
                                        .print();

        env.execute();
    }
}

