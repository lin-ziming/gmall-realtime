package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        loginDataStream.print();
    
    
        // 2. 找到今天首次登录和今天7回流首次登录
        
        // 3. 开窗集合
        
        // 4. 写出的clickhouse中
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
