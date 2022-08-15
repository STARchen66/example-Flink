package dataStreamApi;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/*
富函数类（Rich Function Classes）
“富函数类”也是 DataStream API 提供的一个函数类的接口，所有的 Flink 函数类都有其Rich 版本。富函数类一般是以抽象类的形式出现的。
例如：RichMapFunction、RichFilterFunction、RichReduceFunction 等。
既然“富”，那么它一定会比常规的函数类提供更多、更丰富的功能。与常规函数类的不同主要在于，富函数类可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。

Rich Function 有生命周期的概念。典型的生命周期方法有：

open()方法，是 Rich Function 的初始化方法，也就是会开启一个算子的生命周期。当一个算子的实际工作方法例如 map()或者 filter()方法被调用之前，open()会首先被调用。
所以像文件 IO 的创建，数据库连接的创建，配置文件的读取等等这样一次性的工作，都适合在 open()方法中完成。。
close()方法，是生命周期中的最后一个调用的方法，类似于解构方法。一般用来做一些清理工作。
需要注意的是，这里的生命周期方法，对于一个并行子任务来说只会调用一次；而对应的，实际工作方法，例如 RichMapFunction 中的 map()，在每条数据到来后都会触发一次调用。
 */
public class TransformRichFunctionTest {
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

        stream.map(new MyRichMapper()).print();

        env.execute();

    }

    // 实现一个自定义的富函数类
    public static class MyRichMapper extends RichMapFunction<Event, Integer>{


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用 " + getRuntimeContext().getIndexOfThisSubtask()  + "号任务启动");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }
        @Override
        public void close() throws Exception {
            super.close();

            System.out.println("close生命周期被调用 " + getRuntimeContext().getIndexOfThisSubtask()  + "号任务结束");
        }
    }
}

