package com.atguigu.realtime.sugar.mapper;

import com.atguigu.realtime.sugar.bean.Province;
import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.bean.Tm;
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
    
}
