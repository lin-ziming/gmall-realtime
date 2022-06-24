package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.TrafficPageViewBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

import static com.atguigu.realtime.common.Constant.*;

/**
 * @Author lzc
 * @Date 2022/6/24 14:44
 */
public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAppV2 {
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().init(
            3002,
            2,
            "Dws_02_DwsTrafficVcChArIsNewPageViewWindow",
            TOPIC_DWD_TRAFFIC_PAGE,
            TOPIC_DWD_TRAFFIC_UV,
            TOPIC_DWD_TRAFFIC_UJ_DETAIL
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> streams) {
        
        
        // 1. 解析, 把多个流union成一个流
        DataStream<TrafficPageViewBean> beanStream = unionOne(streams);
        beanStream.print();
    
        // 2. 开窗聚合
        
        // 3. 写出到clickhouse中
    }
    
    private DataStream<TrafficPageViewBean> unionOne(Map<String, DataStreamSource<String>> streams) {
        streams.get(TOPIC_DWD_TRAFFIC_UJ_DETAIL).print(TOPIC_DWD_TRAFFIC_UJ_DETAIL);
        
        // pv sv  durSum
        SingleOutputStreamOperator<TrafficPageViewBean> pvSvDurSumStream = streams
            .get(TOPIC_DWD_TRAFFIC_PAGE)
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                JSONObject common = obj.getJSONObject("common");
                JSONObject page = obj.getJSONObject("page");
                String vc = common.getString("vc");
                String ch = common.getString("ch");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");
            
                Long pv = 1L;
                Long sv = page.getString("last_page_id") == null ? 1L : 0L;
                Long durSum = page.getLong("during_time");
            
                Long ts = obj.getLong("ts");
            
                return new TrafficPageViewBean("", "",
                                               vc, ch, ar, isNew,
                                               0L, sv, pv, durSum, 0L,
                                               ts
                );
            });
        // uv
        SingleOutputStreamOperator<TrafficPageViewBean> uvStream = streams
            .get(TOPIC_DWD_TRAFFIC_UV)
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                JSONObject common = obj.getJSONObject("common");
                String vc = common.getString("vc");
                String ch = common.getString("ch");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");
            
                Long ts = obj.getLong("ts");
            
                return new TrafficPageViewBean("", "",
                                               vc, ch, ar, isNew,
                                               1L, 0L, 0L, 0L, 0L,
                                               ts
                );
            });
    
        // uv
        SingleOutputStreamOperator<TrafficPageViewBean> ujStream = streams
            .get(TOPIC_DWD_TRAFFIC_UJ_DETAIL)
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                JSONObject common = obj.getJSONObject("common");
                String vc = common.getString("vc");
                String ch = common.getString("ch");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");
            
                Long ts = obj.getLong("ts");
            
                return new TrafficPageViewBean("", "",
                                               vc, ch, ar, isNew,
                                               0L, 0L, 0L, 0L, 1L,
                                               ts
                );
            });
        
        return pvSvDurSumStream.union(uvStream, ujStream);
    
    }
}
/*

不同的维度:


五个指标:
会话数
    页面日志
浏览总时长
    页面日志

页面浏览数  pv
    页面日志
独立访客数  uv
    来源uv详情表
跳出会话数  uj
    来源uj详情表
------------------------

3个流:
union

--------

小米..     1   0   0
小米..     0   1   0
小米..     1   0   0


聚合:
按照维度开窗聚合
  keyBy().window().reduce()...
-----------------------------------
维度    窗口    pv  uv  uj
小米..  0-5     10   3   1
华为..  0-5     20   13   3
 */