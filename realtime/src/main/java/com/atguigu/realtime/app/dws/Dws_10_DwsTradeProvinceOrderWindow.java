package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeProvinceOrderWindow;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.function.DimAsyncFunction;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/6/30 10:27
 */
public class Dws_10_DwsTradeProvinceOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_10_DwsTradeProvinceOrderWindow().init(
            3010,
            2,
            "Dws_10_DwsTradeProvinceOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        
        // 1. ??????orderdetail_id??????
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        // 2. ?????????pojo??????
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStream = parseToPojo(distinctedStream);
        
        // 3. ????????????
        SingleOutputStreamOperator<TradeProvinceOrderWindow> aggregatedStream = windowAndAgg(beanStream);
    
        // 4. ??????????????????
        SingleOutputStreamOperator<TradeProvinceOrderWindow> resultStream = joinDim(aggregatedStream);
    
        // 5. ?????????clickhouse???
        writeToClickhouse(resultStream);
    }
    
    private void writeToClickhouse(SingleOutputStreamOperator<TradeProvinceOrderWindow> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_trade_province_order_window", TradeProvinceOrderWindow.class));
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> joinDim(SingleOutputStreamOperator<TradeProvinceOrderWindow> aggregatedStream) {
       return AsyncDataStream.unorderedWait(
            aggregatedStream,
            new DimAsyncFunction<TradeProvinceOrderWindow>(){
                @Override
                public String getTable() {
                    return "dim_base_province";
                }
    
                @Override
                public String getId(TradeProvinceOrderWindow input) {
                    return input.getProvinceId();
                }
    
                @Override
                public void addDim(TradeProvinceOrderWindow input, JSONObject dim) {
                    input.setProvinceName(dim.getString("NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> windowAndAgg(SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStream) {
       return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeProvinceOrderWindow::getProvinceId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1,
                                                           TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<TradeProvinceOrderWindow> elements,
                                        Collector<TradeProvinceOrderWindow> out) throws Exception {
                        
                        TradeProvinceOrderWindow bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));
                        bean.setOrderCount((long) bean.getOrderIdSet().size());
                        
                        bean.setTs(ctx.currentProcessingTime());
                        
                        out.collect(bean);
                        
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> parseToPojo(
        SingleOutputStreamOperator<JSONObject> stream) {
        
        return stream.map(new MapFunction<JSONObject, TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow map(JSONObject value) throws Exception {
                return TradeProvinceOrderWindow.builder()
                    .provinceId(value.getString("province_id"))
                    .orderIdSet(new HashSet<>(Collections.singleton(value.getString("order_id"))))
                    .orderAmount(value.getDoubleValue("split_total_amount"))
                    .ts(value.getLong("ts") * 1000)
                    .build();
            }
        });
    }
    
    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("id"))
            .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                
                private ValueState<JSONObject> maxDateDataState;
                
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    maxDateDataState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("maxDateDataState", JSONObject.class));
                    
                }
                
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                    // ????????????????????????, ??????????????????????????????????????????????????????: ??????????????????????????????
                    out.collect(maxDateDataState.value());
                }
                
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    if (maxDateDataState.value() == null) {
                        // ?????????????????????
                        // 1. ???????????????: 5s?????????????????????
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                        // 2.????????????
                        maxDateDataState.update(value);
                        
                    } else {
                        // ???????????????
                        // 3. ????????????, ??????????????????????????????, ??????????????????????????????(????????????)
                        // "2022-06-27 01:04:48.839Z"   "2022-06-27 01:04:48.9z"
                        String current = value.getString("row_op_ts");
                        String last = maxDateDataState.value().getString("row_op_ts");
                        // ??????current >= last ???????????????
                        boolean isGreaterOrEqual = AtguiguUtil.compareLTZ(current, last);  // ??????current >= last ?????????true, ????????????false
                        if (isGreaterOrEqual) {
                            maxDateDataState.update(value);
                        }
                        
                    }
                }
            });
    }
}
