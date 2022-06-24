package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseSQLApp;
import com.atguigu.realtime.bean.KeywordBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.function.IkAnalyzer;
import com.atguigu.realtime.util.FlinkSinUtil;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/24 9:13
 */
public class Dws_01_DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new Dws_01_DwsTrafficSourceKeywordPageViewWindow().init(
            3001,
            2,
            "Dws_01_DwsTrafficSourceKeywordPageViewWindow",
            10
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 读取页面日志数据: ddl语句
        tEnv.executeSql("create table page_log(" +
                            " page map<string, string>, " +
                            " ts bigint, " +
                            " et as to_timestamp_ltz(ts, 3), " +
                            "watermark for et as et - interval '3' second" +
                            ")" + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "Dws_01_DwsTrafficSourceKeywordPageViewWindow"));
        
        // 2. 过滤出搜索记录, 取出搜索关键词
        Table t1 = tEnv.sqlQuery("select  " +
                                     " page['item'] keyword, " +
                                     " et " +
                                     "from page_log " +
                                     "where page['last_page_id']='search' " +
                                     "and page['item_type']='keyword' " +
                                     "and page['item'] is not null");
        tEnv.createTemporaryView("t1", t1);
        
        // 3. 对关键词进行分词  自定义函数
        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);
        Table t2 = tEnv.sqlQuery("select " +
                                     " kw, " +
                                     " et " +
                                     "from t1 " +
                                     "join lateral table(ik_analyzer(keyword))on true ");
        tEnv.createTemporaryView("t2", t2);
        
        // 4. 开窗聚合
        // 分组窗口 tvf over
        // 分组窗口: 滚动 滚动 会话
        // tvf: 滚动 滑动  累计
        Table resultTable = tEnv.sqlQuery("select " +
                                              " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                                              " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                                              " 'search' source, " +
                                              " kw keyword, " +
                                              " count(*) keywordCount, " +
                                              " unix_timestamp() * 1000 as ts " +
                                              "from table( tumble( table t2, descriptor(et), interval '5' second ) ) " +
                                              "group by kw, window_start, window_end");
        // 5. 把结果写出到 ClickHouse 中
        // 自定义流到sink
        // 支持jdbc连接
        // bean里面的字段名要和表中的字段名保持一致, 这样才能使用反射的反射方式写入
        tEnv
            .toRetractStream(resultTable, KeywordBean.class)
            .filter(t -> t.f0)
            .map(t -> t.f1)
            .addSink(FlinkSinUtil.getClickHoseSink("dws_traffic_source_keyword_page_view_window",KeywordBean.class ));
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
分词: 自定义函数?
    标量函数(scalar)  udf
    制表函数(table)  udtf
    聚合函数(aggregate)
    制表聚合函数(table aggregate)

苹果手机
    苹果
    手机
    
    table function
    
        pubic void eval(String s){
            collect(...)
            collect(...)
            collect(...)
        }
        
        
 */