package streamConversion;


/*
* 对于连接流 ConnectedStreams 的处理操作，需要分别定义对两条流的处理转换，因此接口中就会有两个相同的方法需要实现，
* 用数字“1”“2”区分，在两条流中的数据到来时分别调用。我们把这种接口叫作“协同处理函数”（co-process function）。
* 与 CoMapFunction 类似，如果是调用.flatMap()就需要传入一个 CoFlatMapFunction，需要实现 flatMap1()、flatMap2()两个方法；
* 而调用.process()时，传入的则是一个 CoProcessFunction。

下面是 CoProcessFunction 的一个具体示例：
* 我们可以实现一个实时对账的需求，也就是app 的支付操作和第三方的支付操作的一个双流 Join。App 的支付事件和第三方的支付事件将会互相等待 5 秒钟，
* 如果等不来对应的支付事件，那么就输出报警信息。
* 程序如下：*/

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //来自app中的支付日志
        SingleOutputStreamOperator<Tuple3<String,String,Long>> appStream = env.socketTextStream("slave02",7777)
                .flatMap((String line,Collector<Tuple3<String,String,Long>> out) -> {
                    String[] files = line.split(",");
                    out.collect(Tuple3.of(files[0],files[1],Long.valueOf(files[2])));
                }).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
//                .fromElements(
//                Tuple3.of("order-1", "app", 1000L),
//                Tuple3.of("order-2", "app", 2000L),
//                Tuple3.of("order-3", "app", 3500L)
                //order-1, app,1000
//        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );
        //来自第三方平台的支付日志
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> thirdPartStream = env.socketTextStream("slave03",7777)
                .flatMap((String line,Collector<Tuple4<String,String,String,Long>> out) -> {
                    String[] files = line.split(",");
                    out.collect(Tuple4.of(files[0],files[1],files[2], Long.valueOf(files[3])));
                }).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.LONG))
/*                .fromElements(
                Tuple4.of("order-1", "third-party","success", 3000L),
                Tuple4.of("order-3", "third-party","success", 4000L)*/
                //order-1, third-party,success,3000
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String,String,String,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                })
        );
        //检测账单在两条流中是否匹配
        appStream.connect(thirdPartStream)
                .keyBy(data ->data.f0,data ->data.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();

    }

    //自定义实现CoProcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String,String,Long>,Tuple4<String,String,String,Long>,String>{

        //定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String,String,Long>> appEventState;
        private ValueState<Tuple4<String,String,String,Long>> thirdPartyEvent;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
            );
            thirdPartyEvent = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String,String,String,Long>>("thirdParty-event",Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value,
                                    CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx,
                                    Collector<String> out) throws Exception {
            //app中的数据先来，监测另一条流的数据是否来过
            if(thirdPartyEvent.value() != null){
                out.collect("对账成功" + value + " " + thirdPartyEvent.value());
                //清空状态
                thirdPartyEvent.clear();
            }else {
                //更新装载
                appEventState.update(value);
                //注册定时器等另一条流事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            //app中的数据先来，监测另一条流的数据是否来过
            if(appEventState.value() != null){
                out.collect("对账成功" + value + " " + appEventState.value());
                //清空状态
                appEventState.clear();
            }else {
                //更新装载
                thirdPartyEvent.update(value);
                //注册定时器等另一条流事件
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器被触发，判断状态，若某个状态不为空，说明数据没来
            if(appEventState.value()!=null){
                out.collect("对账失败: " + appEventState.value() + " " + "未获取到第三方平台支付信息");
            }
            if(thirdPartyEvent.value()!=null){
                out.collect("对账失败: " + thirdPartyEvent.value() + " " + "未获取到app支付信息");
            }
            appEventState.clear();
            thirdPartyEvent.clear();
        }
    }
}
