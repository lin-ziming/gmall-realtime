package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author lzc
 * @Date 2022/6/15 14:25
 */
public class FlinkSinUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    
    }
    
    public static <T>SinkFunction<T> getClickHoseSink(String table) {
        //使用jdbcSink封装一个clickhouse sink
        String driver = Constant.CLICKHOSUE_DRIVER;
        String url = Constant.CLICKHOSUE_URL;
        // insert into table(age, name, sex) values(?,?,?)
        // 使用反射, 找到pojo中的属性名
        String sql = "";
        return getJdbcSink(driver,url, null, null, sql);
    }
    
    private static <T> SinkFunction<T> getJdbcSink(String driver, String url, String user, String password, String sql) {
        
        return JdbcSink.sink(
            sql,
            new JdbcStatementBuilder<T>() {
                @Override
                public void accept(PreparedStatement ps,
                                   T t) throws SQLException {
                    //TODO  要根据sql语句
        
                }
            },
            new JdbcExecutionOptions.Builder()
                .withBatchSize(1024)
                .withBatchIntervalMs(2000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(driver)
                .withUrl(url)
                .withUsername(user)
                .withPassword(password)
                .build()
        );
    }
    
    
}
