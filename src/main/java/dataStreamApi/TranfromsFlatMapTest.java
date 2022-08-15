package dataStreamApi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
3. 扁平映射（flatMap）
flatMap 操作又称为扁平映射，主要是将数据流中的整体（一般是集合类型）拆分成一个一个的个体使用。
消费一个元素，可以产生 0 到多个元素。flatMap 可以认为是“扁平化”（flatten）和“映射”（map）两步操作的结合，
也就是先按照某种规则对数据进行打散拆分，再对拆分后的元素做转换处理
 */
public class TranfromsFlatMapTest {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 1000L),
                new Event("Alice", "./home", 1000L)
        );
        //1. 实现flatMapFunction
        stream.flatMap(new MyFlatMap()).print("1");
        //2. 传入lambda表达式
        stream.flatMap((Event value,Collector<String> out ) -> {
            if (value.user.equals("Mary"))
                out.collect(value.url);
            else if (value.user.equals("BOb")){
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {
        }).print("2");
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event,String>{
        @Override
        public void flatMap(Event event, Collector<String> out){
            out.collect(event.user);
            out.collect(event.url);
            out.collect(event.timestamp.toString());
        }
    }
}
