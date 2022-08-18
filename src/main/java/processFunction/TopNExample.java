package processFunction;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import windowAndTime.UrlCountViewExample;
import windowAndTime.UrlViewCount;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

import  org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/*
处理的都是数据流
到了一个窗口的触发时间，按照key输出好几个统计结果

不需要开开五秒的窗口，只需要等一会就可以了，数据收集齐就可以了
按照窗口信息windowend进行一个分组，分组之后就是当前同一个窗口收集到的所有统计数据，然后基于所有收集到的所有统计数据定一个定时器，
等一会，全部到了之后，把所有数据进行排序输出。

在windowEnd的基础上，多等1ms。因为都是waterMark触发，和乱序数据无关，只要waterMark超过了windowEnd+1MS，我们可以保证所有的数据都到齐了

没有窗口了，我们 应该对后面的算子单独定义一个列表（可以创建状态，状态只跟当前的key 有关系，keyedStated），把所有的数据保存进来。
所以来一个数据，就把它塞进去。而且创建一个按照窗口结束时间+1ms之后的触发的定时器，等到触发定时器的时候确保所有数据都到齐了，
我们就从这个状态列表里提取出来，然后做一个排序输出。

我们需要用Flink把 状态列表管理起来，就必须要运行时环境中获取状态，
任务跑起来之后才有运行时，所有我们在open生命周期中获取状态。
 */


public class TopNExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })));
        // 2. url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(),new UrlCountViewExample.UrlViewCountResult());
        urlCountStream.print("url count");

        // 3. 对于同一窗口统计出的访问量，进行收集和排序
        urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopNProcessResult(2))
                .print();

        env.execute();

    }
    public static class TopNProcessResult extends KeyedProcessFunction <Long,UrlViewCount,String>{

        private int n;

        private ListState<UrlViewCount> urlViewCountListState;

        public TopNProcessResult(int n) {
            this.n = n ;
        }


        //在环境中获取状态
        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            //将数据保存在状态中
            urlViewCountListState.add(value);
            //注册 window+1ms 定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.getCurrentKey() + 1);
        }


        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            //排序
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue()-o1.count.intValue();
                }
            });

            // 包装信息，打印输出
            StringBuilder result = new StringBuilder();
            result.append("---------------------\n");
            result.append(" 窗口结束时间： " + new Timestamp(ctx.getCurrentKey())+"\n");
            // 取list前两个，包装信息输出
            for (int i = 0; i < 2; i++) {
                UrlViewCount currTuple = urlViewCountArrayList.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + currTuple.url + " "
                        + " 访问量 ：" + currTuple.count + " \n";
                result.append(info  );
            }
            result.append("---------------------\n");
            out.collect(result.toString());
        }
    }
}
