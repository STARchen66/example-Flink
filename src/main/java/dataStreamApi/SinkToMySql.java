package dataStreamApi;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Statement;

public class SinkToMySql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        stream.addSink(JdbcSink.sink(
                "insert into clicks (user,url) values(?,?)",
                ((statement, event)->{
                    statement.setString(1,event.user);
                    statement.setString(2,event.url);
                }),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/datatest")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("200202")
                        .build()
        ));

        env.execute();
    }
}

