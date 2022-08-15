package dataStreamApi;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
2. 简单聚合
有了按键分区的数据流 KeyedStream，我们就可以基于它进行聚合操作了。Flink 为我们内置实现了一些最基本、最简单的聚合 API，主要有以下几种：

sum()：在输入流上，对指定的字段做叠加求和的操作。
min()：在输入流上，对指定的字段求最小值。
max()：在输入流上，对指定的字段求最大值。
minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计算指定字段的最小值，其他字段会保留最初第一个数据的值；
         而 minBy()则会返回包含字段最小值的整条数据。
maxBy()：与 max()类似，在输入流上针对指定字段求最大值。两者区别与min()/minBy()完全一致。
 */
public class TransformSimpleAggTest {
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
        // 按键分组之后进行聚合，提取当前用户最近一次方访问数据
        stream.keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.user;
                    }
                }).max("timestamp")
                .print("max: ");

        stream.keyBy(data -> data.user)
                .maxBy("timestamp")
                .print("maxBy: ");

        env.execute();

    }
}
