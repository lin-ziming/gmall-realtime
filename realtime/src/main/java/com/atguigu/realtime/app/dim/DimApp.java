package com.atguigu.realtime.app.dim;

import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        stream.print();
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




 */