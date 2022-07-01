package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.Province;
import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.bean.Tm;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/7/1 10:03
 */
public interface TradeService {
    Double gmv(int date);
    
    List<Spu> gmvBySpu(int date);
    
    
    List<Tm> gmvByTm(int date);
    
    List<Province> statsByProvince(int date);
}
