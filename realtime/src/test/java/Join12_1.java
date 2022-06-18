import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/18 14:15
 */
public class Join12_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table s12(id string, name string,age int)with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            " 'properties.group.id' = 'atguigu1', " +
                            " 'topic' = 's12'," +
                            " 'format'='json' " +
                            ")");
        
        
        
        tEnv.sqlQuery("select * from s12").execute().print();
    }
    
}
