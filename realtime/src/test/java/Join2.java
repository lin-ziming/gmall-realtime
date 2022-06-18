import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/18 11:22
 */
public class Join2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        tEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "10 s");
        
        tEnv.executeSql("create table s1(id string, name string)with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            " 'properties.group.id' = 'atguigu', " +
                            " 'topic' = 's1'," +
                            " 'format'='csv' " +
                            ")");
    
        tEnv.executeSql("create table s2(id string, age int)with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            " 'properties.group.id' = 'atguigu', " +
                            " 'topic' = 's2', " +
                            " 'format'='csv' " +
                            ")");
        
        
        // 内连接
        
        tEnv.sqlQuery("select " +
                          " s1.id, " +
                          " name, " +
                          " age " +
                          "from s1 " +
                          "left join s2 on s1.id=s2.id")
            .execute()
            .print();
            
        }
        
    
    
        
    
}
