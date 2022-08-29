package tableAPIAndSql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CommonAPITest {
    public static void main(String[] args) {
        //创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()//创建一个构建器，用于创建EnvironmentSettings的实例。默认情况下，它不指定必需的规划器，而是使用通过discovery在classpath中提供的规划器。
                .inStreamingMode()//设置组件应该在流模式下工作。默认启用
                .useBlinkPlanner()//设置闪烁规划器为所需模块。 这是默认行为
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //创建表
        String createDDL = "create table clickTable (" +
                " user_name String," +
                " url String, " +
                " ts bigint "+
                ") with ( "+
                " 'connector'='filesystem',"+
                "'path'='input/clicks.txt',"+
                "'format'='csv'"+
                ")";
        tableEnv.executeSql(createDDL);

        //调用table api进行表的查询转换
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));
        tableEnv.createTemporaryView("result2",resultTable);//将表API对象注册为临时视图，类似于SQL临时视图。

        //执行sql进行表的查询
        Table resultTable2 = tableEnv.sqlQuery("select url,user_name from result2");

        //创建一张用于输出的表
        String createOutDDl = "create table outTable (" +
                " user_name String," +
                " url String, " +
                ") with ( "+
                "'connector'='filesystem',"+
                "'path'='output',"+
                "'format'='csv'"+
                ")";
        tableEnv.sqlQuery(createOutDDl);

        //创建用于控制台打印输出的表
        String createPrintOutDDl = "create table outTable (" +
                " user_name String," +
                " url String, " +
                ") with ( "+
                "'connector'='filesystem',"+
                "'path'='output',"+
                "'format'='csv'"+
                ")";
        tableEnv.sqlQuery(createPrintOutDDl);

        //输出表
        resultTable.executeInsert("outTable");
        resultTable2.executeInsert("printOutTable");

    }
}
