package dataStreamApi;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
2.过滤（filter）
 ilter 转换操作，顾名思义是对数据流执行一个过滤，
 通过一个布尔条件表达式设置过滤条件，对于每一个流内元素进行判断，
 若为 true 则元素正常输出，若为 false 则元素被过滤掉
 */
public class TransfromFilterTest {
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
        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyFilter());
        result1.print();
        // 2. 传入一个匿名类实现 FilterFunction接口
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("BOb");
            }
        });
        result2.print();
        // 3. 传入一个匿名函数
        stream.filter(data -> data.user.equals("Alice")).print("lamdba: Alice click");

        env.execute();
    }
    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Mary");
        }
    }
}
