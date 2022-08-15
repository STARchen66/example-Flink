package dataStreamApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
1. 映射（map）
对数据一一对应的转换计算。

我们只需要基于 DataStrema 调用 map()方法就可以进行转换处理。
方法需要传入的参数是接口 MapFunction 的实现；返回值类型还是 DataStream，不过泛型（流中的元素类型）可能改变。

使用自定义类 ，实现MayFunction接口
使用匿名类实现MapFunction接口
传入 Lambda表达式
 */

public class TransformMapTest {
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
        //进行转换计算 提取user字段
        //1. 使用自定义类实现MayFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());
        //2. 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event){
                return event.user;
            }
        });
        //3. 传入lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(data ->data.user);

        result1.print();
        result2.print();
        result3.print();
        env.execute();
    }
    //自定义mapfunction
    public static class MyMapper implements MapFunction<Event,String>{
        @Override
        public String map(Event event){
            return event.user;
        }
    }
}