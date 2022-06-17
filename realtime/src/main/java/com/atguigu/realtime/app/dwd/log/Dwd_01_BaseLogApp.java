package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/17 8:51
 */
public class Dwd_01_BaseLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_01_BaseLogApp().init(2001, 2, "Dwd_01_BaseLogApp", Constant.TOPIC_ODS_LOG);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 数据清洗
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        // 2. 纠正新老客户 标记
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etledStream);
        validatedStream.print();
        
        // 3. 分流
        
        // 4. 不同的写入到不同的topic中
        
    }
    
    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> stream) {
        /*
        什么叫新用户, 什么叫老用户?
        
        什么样的用户才有可能标记错?
            
            如果来的表示是老用户, 肯定不会错
            
            如果来的是新用户, 有可能是错的. 老用户被误标记了为新用户.  比如 app卸载重装
            
            状态存储: 用户第一次的访问的日期 年月日
            
            is_new = 0
                不用操作
            
            is_new = 1
              状态是null
                把今天存入到状态中
                
              状态不是null
                
                    今天和状态一样
                        不用修复
                    
                    状态和今天不一样
                       证明今天不是第一天, 不是新用户, 把 is_new = 0
                
            
            
            
         */
        return stream
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                
                private ValueState<String> firstVisitState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitState", String.class));
                   
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    JSONObject common = obj.getJSONObject("common");
                    String isNew = common.getString("is_new");
                    Long ts = obj.getLong("ts");
                    
    
                    String firstVisitDate = firstVisitState.value();
                    String today = DateFormatUtil.toDate(ts);
                    
                    if ("1".equals(isNew)) {
                        // 如果状态是null, 则表示是第一条访问记录: 把今天的日期存入状态中
                        if (firstVisitDate == null) {
                            firstVisitState.update(today);
                        }else{
                            if (!today.equals(firstVisitDate)) {
                                // 今天不是第一次访问
                                common.put("is_new", "0");
                            }
                        }
                    } else if(firstVisitDate == null) {
                        // 来的是老用户, 状态中应该一定有值.
                        // 如果没有值, 则应该给状态添加一个以前的日期(昨天), 用于以后的纠正
                        String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
                        firstVisitState.update(yesterday);
                    }
    
                    out.collect(obj);
                    
                }
            });
        
        
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .filter(json -> {
                try {
                    JSON.parseObject(json);
                    return true;
                } catch (Exception e) {
                    System.out.println("数据格式不是json, 请检查....");
                    return false;
                }
            })
            .map(JSON::parseObject);
    }
}
