package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeSkuOrderBean;
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
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/6/27 10:20
 */
public class Dws_09_DwsTradeSkuOrderWindow_Cache_Async extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_Cache_Async().init(
            3009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. ??????order_detail_id????????????
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        
        // 2. ??????????????????????????????, ?????????pojo???
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parsetToPojo(distinctedStream);
        // 3. ??????sku_id ????????????????????????
        SingleOutputStreamOperator<TradeSkuOrderBean> streamWithoutDim = windowAndAggregate(beanStream);
        
        // 4. ??????????????????
        SingleOutputStreamOperator<TradeSkuOrderBean> streamWithDim = joinDim(streamWithoutDim);
    
        // 5. ????????????clickhouse???
        writeToClickHouse(streamWithDim);
    }
    
    private void writeToClickHouse(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        stream.addSink(FlinkSinkUtil.getClickHoseSink("dws_trade_sku_order_window", TradeSkuOrderBean.class));
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(
            stream,
            new DimAsyncFunction<TradeSkuOrderBean>(){
                @Override
                public String getTable() {
                    return "dim_sku_info";
                }
    
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getSkuId();
                }
    
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setSkuName(dim.getString("SKU_NAME")); // {"SPU_ID": "1", "SKU_NAME": "abc"}
                    bean.setSpuId(dim.getString("SPU_ID"));
                    bean.setTrademarkId(dim.getString("TM_ID"));
                    bean.setCategory3Id(dim.getString("CATEGORY3_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = AsyncDataStream.unorderedWait(
            skuInfoStream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_trademark";
                }
            
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getTrademarkId();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setTrademarkName(dim.getString("TM_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
        SingleOutputStreamOperator<TradeSkuOrderBean> spuStream = AsyncDataStream.unorderedWait(
            tmStream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_spu_info";
                }
            
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getSpuId();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setSpuName(dim.getString("SPU_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
    
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
            spuStream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category3";
                }
            
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getCategory3Id();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setCategory3Name(dim.getString("NAME"));
                    bean.setCategory2Id(dim.getString("CATEGORY2_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
            c3Stream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category2";
                }
            
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getCategory2Id();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setCategory2Name(dim.getString("NAME"));
                    bean.setCategory1Id(dim.getString("CATEGORY1_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        return AsyncDataStream.unorderedWait(
            c2Stream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category1";
                }
        
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getCategory1Id();
                }
        
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setCategory1Name(dim.getString("NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
    
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAggregate(
        SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeSkuOrderBean::getSkuId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1,
                                                    TradeSkuOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                        value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                        value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<TradeSkuOrderBean> elements,
                                        Collector<TradeSkuOrderBean> out) throws Exception {
                        
                        TradeSkuOrderBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        // ??????set????????????????????????orderCount??????
                        bean.setOrderCount((long) bean.getOrderIdSet().size());
                        
                        out.collect(bean);
                        
                        
                    }
                }
            );
        
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> parsetToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        
        return stream.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            
            @Override
            public TradeSkuOrderBean map(JSONObject value) throws Exception {
                TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                    .skuId(value.getString("sku_id"))
                    // ?????????????????????null, ????????????0D
                    .originalAmount(value.getDoubleValue("split_original_amount"))
                    .activityAmount(value.getDoubleValue("split_activity_amount"))
                    .couponAmount(value.getDoubleValue("split_coupon_amount"))
                    .orderAmount(value.getDoubleValue("split_total_amount"))
                    .ts(value.getLong("ts") * 1000)
                    .build();
                
                bean.getOrderIdSet().add(value.getString("order_id"));
                
                
                return bean;
                
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
/*
????????????:


????????????:
    ??????????????????, ??????????????????, ???????????????????????????...
    
    ?????????????????????:
    
    
   ?????????+????????????
    ???????????????????????????????????????????????????

*/