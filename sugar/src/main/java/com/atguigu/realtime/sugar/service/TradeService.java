package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.Spu;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/7/1 10:03
 */
public interface TradeService {
    Double gmv(int date);
    
    List<Spu> gmvBySpu(int date);
}
