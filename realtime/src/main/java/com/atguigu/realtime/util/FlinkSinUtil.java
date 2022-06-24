package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.KeywordBean;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author lzc
 * @Date 2022/6/15 14:25
 */
public class FlinkSinUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
        
    }
    
    public static <T> SinkFunction<T> getClickHoseSink(String table, Class<T> tClass) {
        //使用jdbcSink封装一个clickhouse sink
        String driver = Constant.CLICKHOSUE_DRIVER;
        String url = Constant.CLICKHOSUE_URL;
        // insert into table(age, name, sex) values(?,?,?)
        // 使用反射, 找到pojo中的属性名
        
        Field[] fields = tClass.getDeclaredFields();
        
        /*String names = "";
        for (Field field : fields) {
            String name = field.getName();
            names += name + ",";
        }
        names = names.substring(0, names.length() - 1);*/
        
        String names = Stream
            .of(fields)
            .map(field -> {
                String name = field.getName();
                return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);  // 驼峰转成下划线
            })
            .collect(Collectors.joining(","));
        
        
        String sql = "insert into " + table + "(" + names + ")values(" + names.replaceAll("[^,]+", "?") + ")";
        System.out.println("clickhosue 插入语句:" + sql);
        return getJdbcSink(driver, url, null, null, sql);
    }
    
    public static void main(String[] args) {
        getClickHoseSink("a", KeywordBean.class);
    }
    
    private static <T> SinkFunction<T> getJdbcSink(String driver, String url, String user, String password,
                                                   String sql) {
        
        return JdbcSink.sink(
            sql,
            new JdbcStatementBuilder<T>() {
                @Override
                public void accept(PreparedStatement ps,
                                   T t) throws SQLException {
                    //TODO  要根据sql语句
                    // insert into a(stt,edt,source,keyword,keyword_count,ts)values(?,?,?,?,?,?)
                    Class<?> tClass = t.getClass();
                    Field[] fields = tClass.getDeclaredFields();
                    try {
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            field.setAccessible(true);
                            Object v = field.get(t);
                            ps.setObject(i + 1, v);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    
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
