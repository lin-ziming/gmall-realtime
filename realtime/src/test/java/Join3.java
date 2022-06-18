import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/18 14:40
 */
public class Join3 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        tEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "10 s");
    
        // 事实表
        tEnv.executeSql("create table s1(" +
                            " id bigint," +
                            " name string, " +
                            " pt as proctime()" +
                            ")with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            " 'properties.group.id' = 'atguigu', " +
                            " 'topic' = 's1'," +
                            " 'format'='csv' " +
                            ")");
        
        
        tEnv.executeSql("create table user_info(" +
                            "id bigint, " +
                            "gender string " +
                            ")with(" +
                            "   'connector' = 'jdbc',\n" +
                            "   'url' = 'jdbc:mysql://hadoop162:3306/gmall2022',\n" +
                            "   'table-name' = 'user_info'," +
                            "   'username' = 'root'," +
                            "'lookup.cache.max-rows' = '10',\n" +
                            "'lookup.cache.ttl' = '30 s',\n" +
                            "   'password' = 'aaaaaa'" +
                            ")");
        
        tEnv.sqlQuery("select " +
                          " s1.id, " +
                          " name, " +
                          " ui.gender " +
                          "from s1 " +
                          "join user_info for system_time as of s1.pt ui on s1.id=ui.id")
            .execute()
            .print();
        
        
    }
}
