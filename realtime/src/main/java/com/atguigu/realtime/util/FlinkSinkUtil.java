package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/6/17 11:02
 */
public class FlinkSinkUtil {
    public static SinkFunction<String> getKafkaSink(String topic) {
        
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("transaction.timeout.ms", 15 * 60 * 1000);
        
        
        return new FlinkKafkaProducer<String>(
            "default",
            new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                @Nullable Long timestamp) {
                    return new ProducerRecord<>(topic,element.getBytes(StandardCharsets.UTF_8));
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
