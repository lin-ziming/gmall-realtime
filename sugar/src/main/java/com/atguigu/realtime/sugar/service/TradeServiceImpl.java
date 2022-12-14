package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.*;
import com.atguigu.realtime.sugar.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/7/1 10:03
 */
@Service
public class TradeServiceImpl implements TradeService{
    
    @Autowired
    TradeMapper tradeMapper;
    @Override
    public Double gmv(int date) {
        return tradeMapper.gmv(date);
    }
    
    @Override
    public List<Spu> gmvBySpu(int date) {
        return tradeMapper.gmvBySpu(date);
    }
    
    @Override
    public List<Tm> gmvByTm(int date) {
        return tradeMapper.gmvByTm(date);
    }
    
    @Override
    public List<Province> statsByProvince(int date) {
        return tradeMapper.statsByProvince(date);
    }
    
    @Override
    public List<Traffic> statsPVUvSV(int date) {
        return tradeMapper.statsPVUvSV(date);
    }
    
    @Override
    public List<Kw> statsKw(int date) {
        return tradeMapper.statsKw(date);
    }
    
    
}
