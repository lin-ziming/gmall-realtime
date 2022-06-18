package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

/**
 * @Author lzc
 * @Date 2022/6/18 15:39
 */
public class SQLUtil {
    
    public static String getKafkaSourceDDL(String topic, String groupId) {
        return "with(" +
            " 'connector'='kafka', " +
            " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
            " 'properties.group.id'='" + groupId + "', " +
            " 'format'='json', " +
            " 'topic'='" + topic + "' " +
            ")";
    }
    
    public static String getKafkaSinkDDL(String topic) {
        return "with(" +
            " 'connector'='kafka', " +
            " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
            " 'format'='json', " +
            " 'topic'='" + topic + "' " +
            ")";
    }
}
