package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeSkuOrderBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/27 10:20
 */
public class Dws_09_DwsTradeSkuOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow().init(
            3009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先找order_detail_id进行去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        
        // 2. 根据需要去除一些字段, 封装到pojo中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parsetToPojo(distinctedStream);
        // 3. 按照sku_id 进行分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> streamWithoutDim = windowAndAggregate(beanStream);
        
        // 4. 补充维度信息
        joinDim(streamWithoutDim);
        
        // 5. 写出的奥clickhouse中
    }
    
    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        
        stream
            .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
    
                private Connection phoenixConn;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 获取phoenix连接
                    phoenixConn = JdbcUtil.getPhoenixConnection();
                }
    
                @Override
                public void close() throws Exception {
                    if (phoenixConn != null) {
                        phoenixConn.close();
                    }
                }
    
                @Override
                public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                    // 1. 根据sku_id 查询出来3个id:  spu_id 和 tm_id 和 c3_id
                    // 查询sku_info 所有的维度信息   // key: 字段名 value: 值
                    JSONObject skuInfo = DimUtil.readDimFromPhoenix(phoenixConn, "dim_sku_info", bean.getSkuId());
                    bean.setSkuName(skuInfo.getString("SKU_NAME")); // {"SPU_ID": "1", "SKU_NAME": "abc"}
                    bean.setSpuId(skuInfo.getString("SPU_ID"));
                    bean.setTrademarkId(skuInfo.getString("TM_ID"));
                    bean.setCategory3Id(skuInfo.getString("CATEGORY3_ID"));
                    
                    // 2. base_trademark
                    JSONObject baseTrademark = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_trademark", bean.getTrademarkId());
                    bean.setTrademarkName(baseTrademark.getString("TM_NAME"));
                    
                    // 3. spu
                    JSONObject spuInfo = DimUtil.readDimFromPhoenix(phoenixConn, "dim_spu_info", bean.getSpuId());
                    bean.setSpuName(spuInfo.getString("SPU_NAME"));
                    
                    // 4. c3
                    JSONObject c3 = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_category3", bean.getCategory3Id());
                    bean.setCategory3Name(c3.getString("NAME"));
                    bean.setCategory2Id(c3.getString("CATEGORY2_ID"));
                    
                    // 5. c2
                    JSONObject c2 = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_category2", bean.getCategory2Id());
                    bean.setCategory2Name(c2.getString("NAME"));
                    bean.setCategory1Id(c2.getString("CATEGORY1_ID"));
                    
                    // 6. c1
                    JSONObject c1 = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_category1", bean.getCategory1Id());
                    bean.setCategory1Name(c1.getString("NAME"));
                    
                    return bean;
                }
            })
            .print();
        
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
                        
                        // 根据set集合的长度去设置orderCount的值
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
                    // 如果字段的值是null, 则会赋值0D
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
                    // 定时器触发的时候, 状态中保存的一定是时间最大的那条数据: 最后一个最完整的数据
                    out.collect(maxDateDataState.value());
                }
                
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    if (maxDateDataState.value() == null) {
                        // 第一条数据进来
                        // 1. 注册定时器: 5s后触发的定时器
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                        // 2.更新状态
                        maxDateDataState.update(value);
                        
                    } else {
                        // 不是第一条
                        // 3. 比较时间, 如果新来的时间比较大, 则把这条数据保存下来(更新状态)
                        // "2022-06-27 01:04:48.839Z"   "2022-06-27 01:04:48.9z"
                        String current = value.getString("row_op_ts");
                        String last = maxDateDataState.value().getString("row_op_ts");
                        // 如果current >= last 则更新状态
                        boolean isGreaterOrEqual = AtguiguUtil.compareLTZ(current, last);  // 如果current >= last 则返回true, 否则返回false
                        if (isGreaterOrEqual) {
                            maxDateDataState.update(value);
                        }
                        
                    }
                }
            });
    }
}
/*
数据源  详情表
    详情表来源于预处理表
            预处理表中有左连接
                有重复数据


去重
    有一条详情数据出现大量重复
        粒度: sku
            detail_1  sku_1  100        null
            detail 1  sku_1  100        10

     按照 detail_1 去重
        保留最后那一条
            时间戳最大


  1.   如何找到最后一条?
       详情表 left join 详情活动 left join 详情优惠券
        5s内, 如果有的话, 一定会来齐

        定时器:
            第一条来的时候注册定时器
                中间每来一条数据, 就判断时间, 如果来的时间大于状态中存储的那个时间, 更新

            等定时器触发的时候, 最后一条一定来了


  2.    去重简化:
        为什么一定要用最后一条? 因为最后一条的数据比较完整
            如果在统计计算的时候, 根本就用不到右表的数据, 这种情况只要第一条就行了.


  3.   用窗口 session
     gap 5s
     当窗口关闭的时候, 同一个详情id的数据一定都到了了,再从这个窗口内扎到操作时间最大那个
        时效性比较低

按照 sku 分组, 开窗 聚合

补充维度信息:
    详情中只有一个sku_id
      sku_name
      spu
      tm
      ...


最后写入到clickhouse中



 */