package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.UserLoginBean;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/25 10:33
 */
public class Dws_04_DwsUserUserLoginWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_04_DwsUserUserLoginWindow().init(
            3004,
            2,
            "Dws_04_DwsUserUserLoginWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 过滤出来登录记录
        SingleOutputStreamOperator<JSONObject> loginDataStream = filterLoginData(stream);
        // 2. 找到今天首次登录和今天7回流首次登录
        SingleOutputStreamOperator<UserLoginBean> beanStream = findUvAndBack(loginDataStream);
        
        // 3. 开窗集合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAggregate(beanStream);
    
        // 4. 写出的clickhouse中
        writeToClickhouse(resultStream);
    }
    
    private void writeToClickhouse(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_user_user_login_window", UserLoginBean.class));
    }
    
    private SingleOutputStreamOperator<UserLoginBean> windowAndAggregate(SingleOutputStreamOperator<UserLoginBean> beanStream) {
      return  beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1,
                                                UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
    
                    @Override
                    public void process(Context ctx,
                                        Iterable<UserLoginBean> elements,
                                        Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean bean = elements.iterator().next();
                        
                        bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<UserLoginBean> findUvAndBack(
        SingleOutputStreamOperator<JSONObject> loginDataStream) {
        return loginDataStream
            .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
            .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                
                private ValueState<String> lastLoginDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 记录该用户最后一次登录的年月日
                    lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<UserLoginBean> out) throws Exception {
                    
                    Long ts = obj.getLong("ts");
                    String today = DateFormatUtil.toDate(ts);
                    String lastLoginDate = lastLoginDateState.value();
                    
                    
                    long uuCt = 0;
                    long backCt = 0;
                    
                    if (!today.equals(lastLoginDate)) {
                        System.out.println("uid: " + ctx.getCurrentKey());
                        uuCt = 1;
                        // 把today存储到状态中
                        lastLoginDateState.update(today);
                        
                        if (lastLoginDate != null) {  // 表示曾经登录过, 才有有必要判断是否为回流用户
                            Long lastLoginTs = DateFormatUtil.toTs(lastLoginDate);
                            // 判断是否为7日回流
                            // 今天 - 最后一次登录 > 7天
                            if ((ts - lastLoginTs) / 1000 / 60 / 60 / 24 > 7) {
                                backCt = 1;
                            }
                        }
                        
                        
                    }
                    
                    UserLoginBean bean = new UserLoginBean("", "", backCt, uuCt, ts);
                    
                    if (uuCt == 1) {
                        out.collect(bean);
                    }
                }
            });
        
    }
    
    private SingleOutputStreamOperator<JSONObject> filterLoginData(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .filter(obj -> {
                String uid = obj.getJSONObject("common").getString("uid");
                JSONObject page = obj.getJSONObject("page");
                String pageId = page.getString("page_id");
                String lastPageId = page.getString("last_page_id");
                // uid != null && lastPageId == null 表示实现的是自动登录
                // uid != null && lastPageId == 'login' 表示中途登录
                
                return uid != null && (lastPageId == null || "login".equals(lastPageId));
            });
    }
}
