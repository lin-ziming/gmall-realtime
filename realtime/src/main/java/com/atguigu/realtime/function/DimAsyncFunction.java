package com.atguigu.realtime.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.Executor;

/**
 * @Author lzc
 * @Date 2022/6/29 14:14
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T>{
    
    private Executor threadPool;
    private DruidDataSource druidDataSource;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();
    
        druidDataSource = DruidDSUtil.getDruidDataSource();
    }
    
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {
        // 多线程(线程池)+多客户端
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 读取维度数据
                Jedis redisClient = RedisUtil.getRedisClient();
                Connection phoenixConn = null;
                try {
                    phoenixConn = druidDataSource.getConnection();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
    
                // 使用客户端去读取维度
                JSONObject dim = DimUtil.readDim(redisClient, phoenixConn, getTable(), getId(input));
                
                // 把读到的维度数据存入到 input对象中
                addDim(input, dim);
                
                resultFuture.complete(Collections.singletonList(input));
                
    
                if (redisClient != null) {
                    redisClient.close();
                }
    
                if (phoenixConn != null) {
                    try {
                        phoenixConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        
    }
}
