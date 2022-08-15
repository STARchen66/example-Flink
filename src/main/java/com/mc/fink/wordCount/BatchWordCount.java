package com.mc.fink.wordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/word.txt");
        //3.将每行数据分词转成二元数组
        FlatMapOperator<String, Tuple2<String,Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
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
        UnsortedGrouping<Tuple2<String ,Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        //分组内进行聚合统计
        AggregateOperator<Tuple2<String,Long>> sun = wordAndOneGroup.sum(1);
        //打印输出
        sun.print();
    }
}
