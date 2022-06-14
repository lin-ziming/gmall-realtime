package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/6/14 16:33
 */
public class FlinkSourceUtil {
    
    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        
        
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("group.id", groupId);
        props.put("isolation.level", "read_committed");
        
        
        return new FlinkKafkaConsumer<String>(
            topic,
            new SimpleStringSchema(),
            props
        );
    }
}
