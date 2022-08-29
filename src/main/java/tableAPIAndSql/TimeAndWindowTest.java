package tableAPIAndSql;

import dataStreamApi.ClickSource;
import dataStreamApi.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        // 2. 在流转换成Table时定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        Table clickTable = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());

//        clickTable.printSchema();

        // 聚合查询转换

        // 1. 分组聚合
        Table aggTable = tableEnv.sqlQuery("SELECT user_name, COUNT(1) FROM clickTable GROUP BY user_name");

        // 2. 分组窗口聚合
        Table groupWindowResultTable = tableEnv.sqlQuery("SELECT " +
                "user_name, " +
                "COUNT(1) AS cnt, " +
                "TUMBLE_END(et, INTERVAL '10' SECOND) as endT " +
                "FROM clickTable " +
                "GROUP BY " +                     // 使用窗口和用户名进行分组
                "  user_name, " +
                "  TUMBLE(et, INTERVAL '10' SECOND)" // 定义1小时滚动窗口
        );

        // 3. 窗口聚合
        // 3.1 滚动窗口
        Table tumbleWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  TUMBLE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 3.2 滑动窗口
        Table hopWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  HOP( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 3.3 累积窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  CUMULATE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 4. 开窗聚合
        Table overWindowResultTable = tableEnv.sqlQuery("SELECT user_name, " +
                " avg(ts) OVER (" +
                "   PARTITION BY user_name " +
                "   ORDER BY et " +
                "   ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
                ") AS avg_ts " +
                "FROM clickTable");

        // 结果表转换成流打印输出
//        tableEnv.toChangelogStream(aggTable).print("agg: ");
//        tableEnv.toDataStream(groupWindowResultTable).print("group window: ");
//        tableEnv.toDataStream(tumbleWindowResultTable).print("tumble window: ");
//        tableEnv.toDataStream(hopWindowResultTable).print("hop window: ");
//        tableEnv.toDataStream(cumulateWindowResultTable).print("cumulate window: ");
        tableEnv.toDataStream(overWindowResultTable).print("over window: ");

        env.execute();
    }
}

