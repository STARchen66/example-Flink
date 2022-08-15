package dataStreamApi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import java.util.ArrayList;

public class sourceTest {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //从文件中获取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/word.txt");
        //从集合中获取数据 todo 先创建填充集合再调用env读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(4);
        nums.add(5);
        nums.add(6);
        DataStreamSource<Integer> stream2 = env.fromCollection(nums);
        //对象数组
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Aaa","./home",1000L));
        events.add(new Event("Bbb","./cart",1000L));
        events.add(new Event("Ccc","./home",1000L));
        DataStreamSource<Event> stream3 = env.fromCollection(events);

        stream1.print("1");
        stream2.print();
        stream3.print();
        env.execute();
    }
}
