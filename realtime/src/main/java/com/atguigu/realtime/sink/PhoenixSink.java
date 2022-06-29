package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDSUtil;
import com.atguigu.realtime.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author lzc
 * @Date 2022/6/15 14:27
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    
    private DruidPooledConnection conn;
    private Jedis redisClient;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 用连接池
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();
        conn = druidDataSource.getConnection();
        
        redisClient = RedisUtil.getRedisClient();
        
    }
    
    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();  // 如果是从连接池获取的连接, close是归还
        }
    }
    
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value,
                       Context context) throws Exception {
        // 1. 写数据到phoenix中
        writeToPhoenix(value);
        // 2. 更新缓存或删除缓存
        delCache(value);
        
    }
    
    private void delCache(Tuple2<JSONObject, TableProcess> value) {
    
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
    
    
        // key: 表名:id
//        "update".equals(tp.get)
        if ("update".equals(tp.getOperate_type())) {
            System.out.println("开始删除....");
            String key = tp.getSinkTable() + ":" + data.getString("id");
            redisClient.del(key);
            // 删除的时候,  key不存在怎么办?
        }
    
    }
    
    private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
    
        // upsert into t(a,b,c)values(?,?,?)
        // 1. 拼接sql TODO
        StringBuilder sql = new StringBuilder("upsert into ");
        sql
            .append(tp.getSinkTable())
            .append("(")
            .append(tp.getSinkColumns())
            .append(")values(")
            .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
            .append(")");
        System.out.println("插入语句: " + sql.toString());
        // 2. 根据sql得到 预处理语句
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 3. 给占位符赋值 TODO
        String[] columnNames = tp.getSinkColumns().split(",");
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            Object v = data.get(columnName);
            
            ps.setString(i + 1,v == null ? null : v.toString());  // v == null   null + "" = "null"    "" null
         }
    
        // 4. 执行
        ps.execute();
        // 5. 提交
        conn.commit();
        // 6. 关闭ps
        ps.close();
    }
}
