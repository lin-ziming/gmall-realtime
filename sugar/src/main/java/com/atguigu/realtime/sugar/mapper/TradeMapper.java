package com.atguigu.realtime.sugar.mapper;

import com.atguigu.realtime.sugar.bean.*;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TradeMapper {

    @Select("SELECT sum(order_amount) " +
        "FROM dws_trade_sku_order_window " +
        "WHERE toYYYYMMDD(stt) = #{date}")
    Double gmv(int date);
    
    @Select("SELECT\n" +
        "    spu_name,\n" +
        "    sum(order_amount) AS order_amount\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY spu_name")
    List<Spu> gmvBySpu(int date);
    
    
    @Select("SELECT\n" +
        "    trademark_name,\n" +
        "    sum(order_amount) AS order_amount\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY trademark_name")
    List<Tm> gmvByTm(int date);
    
    
    @Select("SELECT\n" +
        "    province_name,\n" +
        "    sum(order_count) AS order_count,\n" +
        "    sum(order_amount) AS order_amount\n" +
        "FROM dws_trade_province_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY province_name")
    List<Province> statsByProvince(int date);
    
    
    @Select("\n" +
        "SELECT\n" +
        "    toHour(stt) AS hour,\n" +
        "    sum(pv_ct) AS pv,\n" +
        "    sum(uv_ct) AS uv,\n" +
        "    sum(sv_ct) AS sv\n" +
        "FROM dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY toHour(stt)")
    List<Traffic> statsPVUvSV(int date);
    
    
    @Select("SELECT\n" +
        "    keyword,\n" +
        "    sum(keyword_count * multiIf(source = 'search', 10, source = 'order', 8, 5)) AS score\n" +
        "FROM dws_traffic_source_keyword_page_view_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY keyword")
    List<Kw> statsKw(int date);
    
}
