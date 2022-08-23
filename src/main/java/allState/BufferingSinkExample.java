package allState;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/*
1. CheckpointedFunction 接口
在 Flink 中，对状态进行持久化保存的快照机制叫作“检查点”（Checkpoint）。
于是使用算子状态时，就需要对检查点的相关操作进行定义，实现一个 CheckpointedFunction 接口。

public interface CheckpointedFunction {
// 保存状态快照到检查点时，调用这个方法
void snapshotState(FunctionSnapshotContext context) throws Exception
// 初始化状态时调用这个方法，也会在恢复状态时调用
 void initializeState(FunctionInitializationContext context) throws
Exception;
}


每次应用保存检查点做快照时，都会调用.snapshotState()方法，将状态进行外部持久化。
而在算子任务进行初始化时，会调用. initializeState()方法。

使用算子状态去做数据缓存输出的例子
 */

public class BufferingSinkExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.print("data");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));
        env .execute();
    }
    //实现自定义的SingFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction{
        //定义当前类的属性
        private final int threshold;
        public BufferingSink(int threshold){
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        //必须作为算子状态保存起来 才能持久化到磁盘 写入检查点
        private List<Event> bufferedElements;
        //定义一个算子状态
        private ListState<Event> checkPointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);//缓存到列表
            //判断如果到达阈值就批量写入
            if(bufferedElements.size()==threshold){
                //用打印到控制台模拟批量写入
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }
                System.out.println("==============================================\n");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //清空状态
            checkPointedState.clear();
            // 对状态进行持久化，复制缓存列表到列表状态
            for (Event bufferedElement : bufferedElements) {
                checkPointedState.add(bufferedElement);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义算子状态
            ListStateDescriptor<Event> eventListStateDescriptor = new ListStateDescriptor<Event>("buffered-elements",Event.class);
            checkPointedState = context.getOperatorStateStore().getListState(eventListStateDescriptor);
            //如果故障恢复需要将ListState中所有元素复制到本地列表
            if(context.isRestored()){
                for (Event element : checkPointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
