package dataStreamApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
3. 归约聚合（reduce）
与简单聚合类似，reduce 操作也会将 KeyedStream 转换为 DataStream。它不会改变流的元素数据类型，所以输出类型和输入类型是一样的。
调用 KeyedStream 的 reduce 方法时，需要传入一个参数，实现 ReduceFunction 接口。
reduce 同简单聚合算子一样，也要针对每一个 key 保存状态。因为状态不会清空，所以我们需要将 reduce 算子作用在一个有限 key 的流上。
 */

public class TransformReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 2000L),
                new Event("BOb", "./prod?id=100", 3000L),
                new Event("BOb", "./prod?id=18", 3900L),
                new Event("BOb", "./prod?id=140", 4000L),
                new Event("Alice", "./prod?id=140", 5000L),
                new Event("Alice", "./prod?id=240", 20000L),
                new Event("Alice", "./prod?id=040", 5000L)
        );
        // 1. 统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                    }
                });

        // 2. 选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });
        result.print();


        env.execute();

    }
}

