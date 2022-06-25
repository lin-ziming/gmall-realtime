package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.UserRegisterBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/25 11:38
 */
public class Dws_05_DwsUserUserRegisterWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_05_DwsUserUserRegisterWindow().init(
            3005,
            2,
            "Dws_05_DwsUserUserRegisterWindow",
            Constant.TOPIC_DWD_USER_REGISTER
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
        stream
            .map(json -> new UserRegisterBean("", "", 1L, JSONObject.parseObject(json).getLong("ts") * 1000))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window,
                                      Iterable<UserRegisterBean> values,
                                      Collector<UserRegisterBean> out) throws Exception {
    
                        UserRegisterBean bean = values.iterator().next();
                        
                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                    }
                }
            )
            .addSink(FlinkSinkUtil.getClickHoseSink("dws_user_user_register_window", UserRegisterBean.class));
        
    }
}
