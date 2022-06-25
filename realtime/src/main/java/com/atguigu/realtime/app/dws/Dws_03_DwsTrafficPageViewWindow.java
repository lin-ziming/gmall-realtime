package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/25 9:58
 */
public class Dws_03_DwsTrafficPageViewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_03_DwsTrafficPageViewWindow().init(
            3003,
            2,
            "Dws_03_DwsTrafficPageViewWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 先找到奥每个用户 每天的一条首页记录和详情记录
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = findUv(stream);
        
        // 2. 开窗聚合: 没有keyBy , 使用全窗口聚合函数
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndAggregate(beanStream);
        
    
        // 3. 写出clickhouse中
        writeToClickhouse(resultStream);
    }
    
    private void writeToClickhouse(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_traffic_page_view_window", TrafficHomeDetailPageViewBean.class));
    }
    
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAggregate(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
       return beanStream
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1,
                                                                TrafficHomeDetailPageViewBean bean2) throws Exception {
                        bean1.setHomeUvCt(bean1.getHomeUvCt()+bean2.getHomeUvCt());
                        bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt()+bean2.getGoodDetailUvCt());
                        
                        return bean1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window,
                                      Iterable<TrafficHomeDetailPageViewBean> it,
                                      Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean bean = it.iterator().next();
                        
                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        System.out.println(bean);
                        out.collect(bean);
                        
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> findUv(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .filter(obj -> {
                String pageId = obj.getJSONObject("page").getString("page_id");
                return "home".equals(pageId) || "good_detail".equals(pageId);
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                
                private ValueState<String> goodDetailState;
                private ValueState<String> homeState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("homeState", String.class));
                    goodDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetailState", String.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    
                    String pageId = obj.getJSONObject("page").getString("page_id");
                    
                    Long ts = obj.getLong("ts");
                    String today = DateFormatUtil.toDate(ts);
                    
                    long homeUvCt = 0L;
                    long goodDetailUvCt = 0L;
                    
                    // 表示今天第一个home页面来了
                    if ("home".equals(pageId) && !today.equals(homeState.value())) {
                        homeUvCt = 1L;
                        homeState.update(today);
                    } else if ("good_detail".equals(pageId) && !today.equals(goodDetailState.value())) {
                        goodDetailUvCt = 1L;
                        goodDetailState.update(today);
                    }
                    
                    
                    // 两个有一个不为1时候初始数据
                    if (homeUvCt + goodDetailUvCt == 1) {
                        
                        out.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, goodDetailUvCt, ts));
                    }
                    
                }
            });
        
    }
}
