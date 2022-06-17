package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/17 13:49
 */
public class Dwd_02_DwdTrafficUniqueVisitorDetail extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_02_DwdTrafficUniqueVisitorDetail().init(
            2002,
            2,
            "Dwd_02_DwdTrafficUniqueVisitorDetail",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                
                private ValueState<String> firstWindowState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstWindowState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstWindowState", String.class));
                }
                
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<JSONObject> elements,
                                    Collector<JSONObject> out) throws Exception {
                    String yesterday = firstWindowState.value();
                    String today = DateFormatUtil.toDate(ctx.window().getStart());
                    
                    if (!today.equals(yesterday)) {
                        // 变成了第二天, 则先更新状态
                        firstWindowState.update(today);
                        
                        List<JSONObject> list = AtguiguUtil.toList(elements);
                        
                        //                        Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")))
                        JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                        out.collect(min);
                    }
                    
                }
            })
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UV));
    }
}
/*
uv任务: 过滤数据, 一个用户只保留当天的第一条访问记录

----------
数据源:
    启动日志?
        可以. 但是记录会偏小
            通过app访问, 才有启动日志.
            通过浏览器访问是没有启动日志的
    
    页面日志
        选择这个
        
去重的逻辑?
    用一个状态去保存这个用户的最后一次访问的日期
    
    状态是null
        第一次启动, 这条记录保留, 然后更新状态为当前的日期
        
     状态不为null
        状态和当前日期相等
            数据丢弃, 不需要任何的操作
        
        状态和当前日期不相等
            变到了第二天. 这条数据是第二天的第一条数据. 保留. 把状态更新为第二天的日期

考虑乱序问题:
    事件时间+水印
    
    找到当天的第一个窗口
        这个窗口内的数据按照时间排序, 取时间最小的那条数据取出
        
     其他的窗口直接忽略
    
    

*/