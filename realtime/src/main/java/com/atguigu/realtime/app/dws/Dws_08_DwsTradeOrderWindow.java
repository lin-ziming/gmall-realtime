package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeOrderBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @Date 2022/6/27 8:57
 */
public class Dws_08_DwsTradeOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_08_DwsTradeOrderWindow().init(
            3008,
            2,
            "Dws_08_DwsTradeOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("user_id"))
            .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {
                
                private ValueState<String> lastOrderDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDateState", String.class));
                }
                
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<TradeOrderBean> out) throws Exception {
                    System.out.println("user_id: " + ctx.getCurrentKey());
                    
                    Long ts = value.getLong("ts") * 1000;
                    String today = DateFormatUtil.toDate(ts);
                    
                    // 最后一次下单日期
                    String lastOrderDate = lastOrderDateState.value();
                    
                    long uuCt = 0;
                    long newUuCt = 0;
                    
                    if (!today.equals(lastOrderDate)) {
                        // 表示这个用户今天的第一次下单
                        uuCt = 1;
                        lastOrderDateState.update(today);
                        
                        // 判断是否新用户下单
                        // 如果状态是null,证明没有下过单, 就是新用户
                        if (lastOrderDate == null) {
                            newUuCt = 1;
                        }
                    }
                    
                    if (uuCt == 1) {
                        out.collect(new TradeOrderBean("", "",
                                                       uuCt,
                                                       newUuCt,
                                                       ts
                        ));
                    }
                    
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1,
                                                 TradeOrderBean value2) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        return value1;
                    }
                },
                new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window,
                                      Iterable<TradeOrderBean> values,
                                      Collector<TradeOrderBean> out) throws Exception {
                        TradeOrderBean bean = values.iterator().next();
                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
    
                    }
                }
            )
            .addSink(FlinkSinkUtil.getClickHoseSink("dws_trade_order_window", TradeOrderBean.class));
    }
}
