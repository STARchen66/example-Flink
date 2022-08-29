package tableAPIAndSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //在创建表的DDL中直接定义时间属性
        String createDDL  = "create table clickTable (" +
                " `user` String, " +
                " url String, " +
                " ts bigint, " +
                " et AS TO_TIMESTAMP( from_unixtime(ts/1000))," +
                " watermark for et as et-interval '1' second " +
                ") with ( " +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt' , " +
                " 'format' = 'csv'" +
                ") ";

        tableEnv.executeSql(createDDL);

        //普通topN 选取当前所有用户中浏览量最大的两个
        Table topNResultTable = tableEnv.sqlQuery("select user,cnt,row_num " +
                "from (" +
                " select*,row_number() over(" +
                " order by cnt desc" +
                " ) as row_num" +
                " from ( select user,count(url) as cnt from clickTable group by user)" +
                " ) where row_num<=2");

        //窗口topN 一段时间内活跃用户统计
        String subQuery = " select user, count(url) as cnt, window_start, window_end " +
                " from  table ( " +
                " tumble( table clickTable, descriptor(et), interval '10' second )" +
                ") " +
                " group by user, window_start, window_end  ";
        Table windowTopNResultTable = tableEnv.sqlQuery("select user, cnt, row_num, window_end " +
                "from (" +
                " select * , row_number() over(" +
                "        partition by window_start, window_end " +  // 固定写法
                "        order by cnt desc" +
                "       ) as row_num" +
                "       from  ( " + subQuery + " ) " +
                ") where row_num <= 2");
        tableEnv.toDataStream(windowTopNResultTable).print("window Top N ：");
        env.execute();
    }
}
