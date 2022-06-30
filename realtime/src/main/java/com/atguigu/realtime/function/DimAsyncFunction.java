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
/*
异步超时
  其他的原因导致异步处理没有完成, 所以会报超时
  
  1. 首先检查集群 redis hdfs kafka hbase
  2. phoenix能否正常进入
  3. 检查phoenix中的维度表是否全
  4. 确实每张表都有数据
        通过 bootstrap 同步一些维度数据
 
hbase起不来:
    hdfs导致hbase出问题.
    重置
    先停止
        hdfs: 删除目录 /hbase
        zk:  deleteall /hbase
        
     再启动

kakfa出问题:
    删除 logs/ 目录下所有文件
        xcall /opt/module/kafka../logs/*
     zk  删除节点:  deleteall /kafka
 */

