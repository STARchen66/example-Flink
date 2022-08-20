package streamConversion;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/*
连接（connect）。顾名思义，这种操作就是直接把两条流像接线一样对接起来。
两种连接方式

1. 连接流（ConnectedStreams）
为了处理更加灵活，连接操作允许流的数据类型不同。
但我们知道一个 DataStream 中的数据只能有唯一的类型，所以连接得到的并不是 DataStream，而是一个“连接流”（ConnectedStreams）。
连接流可以看成是两条流形式上的“统一”，被放在了一个同一个流中；事实上内部仍保持各自的数据形式不变，彼此之间是相互独立的。
要想得到新的 DataStream，还需要进一步定义一个“同处理”（co-process）转换操作，用来说明对于不同来源、不同类型的数据，怎样分别进行处理转换、得到统一的输出类型。
所以整体上来，两条流的连接就像是“一国两制”，两条流可以保持各自的数据类型、处理方式也可以不同，不过最终还是会统一到同一个 DataStream 中


在代码实现上，需要分为两步：
首先基于一条 DataStream 调用.connect()方法，传入另外一条 DataStream 作为参数，将两条流连接起来，得到一个 ConnectedStreams；
然后再调用同处理方法得到 DataStream。这里可以的调用的同处理方法有.map()/.flatMap()，以及.process()方法。
 */

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(4L, 5L, 6L);
        stream1.connect(stream2)
                .map(new CoMapFunction<Integer, Long, String>() {
                    @Override
                    public String map1(Integer integer) throws Exception {
                        return "Integer: " + integer.toString();
                    }

                    @Override
                    public String map2(Long aLong) throws Exception {
                        return "Long: " + aLong.toString();
                    }
                }).print();
        env.execute();
    }
}

