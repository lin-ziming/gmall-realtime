package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.function.DimAsyncFunction;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * @Date 2022/6/30 13:59
 */
public class Dws_11_TradeTrademarkCategoryUserRefundBean extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_11_TradeTrademarkCategoryUserRefundBean().init(
            3011,
            2,
            "Dws_11_TradeTrademarkCategoryUserRefundBean",
            Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 解析成pojo
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream = parseToPojo(stream);
    
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStreamWithC3IdAndTmId =  joinC3IdAndTmId(beanStream);
        
        // 2. 开窗聚合 需要c3id和tmid
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> aggregatedStream = windowAndAgg(beanStreamWithC3IdAndTmId);
        // 3. 补充维度数据
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = joinDim(aggregatedStream);
        
        // 4. 写出到clickhouse中
        writeToClickHouse(resultStream);
    }
    
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> joinC3IdAndTmId(
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream) {
        return AsyncDataStream.unorderedWait(
            beanStream,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>(){
                @Override
                public String getTable() {
                    return "dim_sku_info";
                }
        
                @Override
                public String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getSkuId();
                }
        
                @Override
                public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                    bean.setTrademarkId(dim.getString("TM_ID"));
                    bean.setCategory3Id(dim.getString("CATEGORY3_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    }
    
    private void writeToClickHouse(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_trade_trademark_category_user_refund_window", TradeTrademarkCategoryUserRefundBean.class));
    }
    
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> joinDim(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> stream) {
      
    
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream.unorderedWait(
            stream,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                public String getTable() {
                    return "dim_base_trademark";
                }
            
                @Override
                public String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getTrademarkId();
                }
            
                @Override
                public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                    bean.setTrademarkName(dim.getString("TM_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
    
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(
            tmStream,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category3";
                }
            
                @Override
                public String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getCategory3Id();
                }
            
                @Override
                public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                    bean.setCategory3Name(dim.getString("NAME"));
                    bean.setCategory2Id(dim.getString("CATEGORY2_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(
            c3Stream,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category2";
                }
            
                @Override
                public String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getCategory2Id();
                }
            
                @Override
                public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                    bean.setCategory2Name(dim.getString("NAME"));
                    bean.setCategory1Id(dim.getString("CATEGORY1_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
        return AsyncDataStream.unorderedWait(
            c2Stream,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category1";
                }
            
                @Override
                public String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getCategory1Id();
                }
            
                @Override
                public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                    bean.setCategory1Name(dim.getString("NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
    }
    
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> windowAndAgg(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream) {
      return  beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(bean -> bean.getTrademarkId() + ":" +  bean.getCategory3Id() + ":" + bean.getUserId())
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1,
                                                                       TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<TradeTrademarkCategoryUserRefundBean> elements,
                                        Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                        TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();
                        
                        bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));
                        
                        bean.setRefundCount((long) bean.getOrderIdSet().size());
    
                        bean.setTs(ctx.currentProcessingTime());
    
                        out.collect(bean);
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> parseToPojo(
        DataStreamSource<String> stream) {
        return stream
            .map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                @Override
                public TradeTrademarkCategoryUserRefundBean map(String value) throws Exception {
                    JSONObject obj = JSON.parseObject(value);
                    return TradeTrademarkCategoryUserRefundBean.builder()
                        .skuId(obj.getString("sku_id"))
                        .userId(obj.getString("user_id"))
                        .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                        .ts(obj.getLong("ts") * 1000)
                        .build();
                }
            });
    }
}
