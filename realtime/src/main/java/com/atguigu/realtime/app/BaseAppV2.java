package com.atguigu.realtime.app;

import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * @Author lzc
 * @Date 2022/6/14 16:47
 */
public abstract class BaseAppV2 {
    public void init(int port, int p, String ckAndGroupId, String firstTopic, String... otherTopics) {
        /*if (topic.length == 0) {
            throw new RuntimeException("你传递了0个topic, 请传递至少1个topic....");
        }*/
        
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        
        
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckAndGroupId);
        
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        List<String> topics = new ArrayList<String>(Arrays.asList(otherTopics));
        topics.add(firstTopic);
    
    
        HashMap<String, DataStreamSource<String>> streams = new HashMap<>();
        for (String topic : topics) {
            
            DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic));
            streams.put(topic, stream);
        }
        
        
        handle(env, streams);
        
        
        try {
            env.execute(ckAndGroupId);  // 定义jobName
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public abstract void handle(StreamExecutionEnvironment env,
                                Map<String, DataStreamSource<String>> streams);
}
