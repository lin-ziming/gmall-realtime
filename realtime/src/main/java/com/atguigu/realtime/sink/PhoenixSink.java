package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Author lzc
 * @Date 2022/6/15 14:27
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    
    private DruidPooledConnection conn;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 用连接池
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();
        conn = druidDataSource.getConnection();
    }
    
    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();  // 如果是从连接池获取的连接, close是归还
        }
    }
    
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 1. 拼接sql
        
        // 2. 根据sql得到 预处理语句
        
        // 3. 给占位符赋值
        
        // 4. 执行
        
        // 5. 提交
        
        // 6. 关闭ps
    }
}
