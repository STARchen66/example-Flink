package com.mc.fink.wordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从参数名提取主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //2.从文件中读取数据
        DataStreamSource<String> lineDataStream = env.socketTextStream(hostname,port);
        //3.将每行数据分词转成二元数组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    //将一行文本进行分词
                    String[] words = line.split(" ");
                    // 将每个单词转换成二元组输出
                    for (String word : words) {
                        out.collect(Tuple2.of(word,1L));
                    }
                })
                //当 lambda 表达式使用 java 泛型时 由于泛型擦除的存在，需要显示声明类型信息
                .returns(Types.TUPLE(Types.STRING,Types.LONG));
        //安装word进行分组
        KeyedStream<Tuple2<String ,Long>,String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);
        //分组内进行聚合统计
        SingleOutputStreamOperator<Tuple2<String,Long>> sum = wordAndOneKeyedStream.sum(1);
        //打印输出
        sum.print();

        //启动执行
        env.execute();
    }
}
