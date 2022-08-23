package allState;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
4. 聚合状态（AggregatingState）
我们举一个简单的例子，对用户点击事件流每 5 个数据统计一次平均时间戳。
这是一个类似计数窗口（CountWindow）求平均值的计算，这里我们可以使用一个有聚合状态的RichFlatMapFunction 来实现
 */

public class AverageTimestampExample {
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

        // 自定义实现平均时间戳的统计
        stream.keyBy(data -> data.user)
                // 可以不用processFunction， 因为之前需要定义定时器，和时间有关
                // 我们只需要简单的转换，自定义状态就可以了, 要用到状态，所以用rich
                .flatMap(new AvgTsResult(5L))
                .print();
        env .execute();
    }

    // 实现自定义的 RichFlatmapFunction
    public static class AvgTsResult extends RichFlatMapFunction<Event, String>{
        private Long count;

        public AvgTsResult(Long count) {
            this.count = count;
        }

        // 定义一个聚合的状态，用来保存平均时间戳
        AggregatingState<Event,Long> avgTsAggState;

        // 定义一个值状态。保存用户访问的次数
        ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event event, Tuple2<Long, Long> longLongTuple2) {
                            return Tuple2.of(longLongTuple2.f0 + event.timestamp, longLongTuple2.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> longLongTuple2) {
                            return longLongTuple2.f0 / longLongTuple2.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            //  每来一条数据， curr count 加1
            Long currCount = countState.value();
            if(currCount== null){
                currCount = 1L;
            }else {
                currCount ++ ;
            }

            // 更新状态
            countState.update(currCount);
            avgTsAggState.add(event);

            // 如果达到count次数就输出结果
            if(currCount.equals(count)){
                collector.collect(event.user + " 过去" + count + "次访问平均时间戳为： " + avgTsAggState.get());
                // 清理状态
                countState.clear();
                avgTsAggState.clear();
            }
        }
    }
}

