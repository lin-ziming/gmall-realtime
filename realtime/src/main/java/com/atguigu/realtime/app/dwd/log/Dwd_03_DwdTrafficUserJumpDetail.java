package com.atguigu.realtime.app.dwd.log;

import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/18 9:12
 */
public class Dwd_03_DwdTrafficUserJumpDetail extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_03_DwdTrafficUserJumpDetail().init(
            2003,
            2,
            "Dwd_03_DwdTrafficUserJumpDetail",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        //
    }
}
