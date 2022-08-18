package processFunction;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

public class TopNProcessAllWindowFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        //直接开窗，所有数据升序排列
        stream.map(data -> data.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(),new UrlAllWindowResult())
                .print();

        env.execute();
    }

    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String,Long>, ArrayList<Tuple2<String,Long>>>{
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            if(accumulator.containsKey(value)){//如果此映射包含指定键的映射，则返回true
                Long count = accumulator.get(value);//返回指定键映射到的值，如果此映射不包含该键的映射，则返回null。
                accumulator.put(value,count + 1);//在这个映射中将指定的值与指定的键关联。如果map之前包含一个对应键的映射，旧的值会被替换。
            }else {
                accumulator.put(value,1L);
            }
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String,Long>> result = new ArrayList<>();
            for (String s : accumulator.keySet()) {//返回包含在此映射中的键的集合视图
                result.add(Tuple2.of(s,accumulator.get(s)));//将指定的元素附加到列表的末尾
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String,Long>>,String,TimeWindow>{

        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context,
                            Iterable<ArrayList<Tuple2<String, Long>>> elements,
                            Collector<String> out) throws Exception {
            ArrayList<Tuple2<String,Long>> list = elements.iterator().next();

            StringBuilder result = new StringBuilder();
            result.append("----------------------------------------\n");
            result.append(" 窗口结束时间： " + new Timestamp(context.window().getEnd()) + "\n");

            for(int i=0;i<2;i++){
                Tuple2<String,Long> currTuple = list.get(i);
                String info = "No. "+(i+1)+" "+"url: "+ currTuple.f0 +" "+"  访问量: "+currTuple.f1 + "\n";
                result.append(info);
            }
            result.append("----------------------------------------\n");
            out.collect(result.toString());
        }
    }
}
