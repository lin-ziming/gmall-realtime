package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * @Author lzc
 * @Date 2022/6/14 16:12
 */
public class DimApp extends BaseAppV1 {
    public static void main(String[] args) {
        
        new DimApp().init(2001, 2, "DimApp", Constant.TOPIC_ODS_DB);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 写具体的业务: 对流数据进行处理
        
        // 1. 对数据做etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        // 2. 读取配置表的数据: flink cdc
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        
        // 3. 数据流和广播流做connect
        connect(etledStream, tpStream);
        
        // 4. 根据配置信息把数据写入到不同的phoenix表中
        
        
    }
    
    private void connect(SingleOutputStreamOperator<JSONObject> dataStream,
                         SingleOutputStreamOperator<TableProcess> tpStream) {
  
        
        // 1. 把配置流做成广播流
        /*
        key:
            mysql中表的表名
        value:
            TableProcess
            
         */
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(tpStateDesc);
        // 2. 数据流去connect广播流
        dataStream
            .connect(bcStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
    
    
                private Connection conn;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 建立到phoenix的连接
                    conn = JdbcUtil.getPhoenixConnection();
                }
    
                @Override
                public void close() throws Exception {
                    // 关闭连接
                    if (conn != null) {
                        conn.close();
                    }
                }
    
                // 处理业务数据
                @Override
                public void processElement(JSONObject value, ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 4. 处理业务数据:从广播状态中读取配置信息. 把数据和配置组成一队, 交给后序的流进行处理
                    
                    
                }
                
                
                // 处理广播流中的数据
                @Override
                public void processBroadcastElement(TableProcess tp,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    
                    
                    // 1. 处理广播数据: 把广播数据写入到广播状态
                    saveTpToState(tp, ctx);
                    // 2. 根据配置信息, 在phoenix中建表
                    checkTable(tp);
                }
    
                private void checkTable(TableProcess tp) throws SQLException {
                    // 去phoenix中建表: jdbc
                    // 1. 拼接sql语句
                    // create table if not exists user(name varchar, age varchar, constraint pk primary key(id))SALT_BUCKETS = 3
                    StringBuilder sql = new StringBuilder("create table if not exists ");
                    sql
                        .append(tp.getSinkTable())
                        .append("(")
                        .append(tp.getSinkColumns().replaceAll("([^,]+)", "$1 varchar"))
                        .append(", constraint pk primary key(")
                        .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())
                        .append("))")
                        .append(tp.getSinkExtend() == null ? "":tp.getSinkExtend());
                    // 2. 通过sql, 得到一个预处理语句: PrepareStatement
                    System.out.println("建表语句: " + sql);
                    PreparedStatement ps = conn.prepareStatement(sql.toString());
                    // 3. 给占位赋值(ddl没有占位符)
                    // 略
                    // 4. 执行
                    ps.execute();
                    
                    // 5. 关闭预处理语句
                    ps.close();
                }
    
                
                
                // 把配置信息存入到状态:  table->Tp
                private void saveTpToState(TableProcess tp,
                                           Context ctx) throws Exception {
                    BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    String key = tp.getSourceTable();
                    state.put(key, tp);
                }
            })
            .print();
        
        
    }
    
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        /*
        读取配置表数据:
            在mysql中的gmall_config->table_process表中
            
            现读取全量, 然后再读取变化的数据
            
            传统的方式:  mysql->maxwell->kafka->flink
            新的姿势: flink cdc
         */
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop162")
            .port(3306)
            .scanNewlyAddedTableEnabled(false) // eanbel scan the newly added tables fature
            .databaseList("gmall_config") // set captured database
            .tableList("gmall_config.table_process") // set captured tables [product, user, address]
            .username("root")
            .password("aaaaaa")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .startupOptions(StartupOptions.initial())
            .build();
        
        return env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
            .map(json -> {
                
                JSONObject obj = JSON.parseObject(json);
                
                return obj.getObject("after", TableProcess.class);
                
            });
        
        
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String json) throws Exception {
                    try {
                        JSONObject obj = JSON.parseObject(json);
                        
                        String type = obj.getString("type");
                        JSONObject data = obj.getJSONObject("data");
                        
                        return "gmall2022".equals(obj.getString("database"))
                            && ("insert".equals(type)
                            || "update".equals(type)
                            || "bootstrap-insert".equals(type))
                            && data != null;
                    } catch (Exception e) {
                        System.out.println("数据格式不是json....");
                        return false;
                    }
                }
            })
            .map(JSON::parseObject);  // 方便后期处理
    }
}
/*
数据从: ods_db

很多的维度信息

如何把不同的维度表写入到phoenix中同的表?

1. phoenix中的表如何创建
    a: 手动创建
        简单. 提前根据需要把表建好
        
        不灵活:
            没有办法根据维度表的变化实时才适应新的变化
            
     b: 动态创建
        提前做好一个配置表, 配备表中配置了所有需要的维度信息.
        flink程序会根据配置信息自动的创建对应的表


        

2. 维度数据入和写入到phoenix中
    
    根据配置信息, 来决定这个条数据应该写入到什么表中



-----
phoenix的盐表:  hbase中的预分区
region

自动分裂:
    0.98之前
        10g的一份为2
        
    0.98之前
        128*2^3 m
        
        
自动迁移:


禁止自动分裂

---------

预分区


 */