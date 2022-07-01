package com.atguigu.realtime.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/7/1 9:28
 */
@RestController
public class SugarController {
    
    // 会自动创建这个类的对象
    @Autowired
    TradeService tradeService;
    
    @RequestMapping("/sugar/gmv")
    public String gmv(int date){
    
        Double gmv = tradeService.gmv(date);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", gmv);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/gmv/spu")
    public String gmvBySpu(int date){
    
        List<Spu> list = tradeService.gmvBySpu(date);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
    
        JSONObject data = new JSONObject();
    
        JSONArray categories = new JSONArray();
    
        for (Spu spu : list) {
            categories.add(spu.getSpu_name());
        }
    
        data.put("categories", categories);
    
    
        JSONArray series = new JSONArray();
    
        JSONObject one = new JSONObject();
        
        one.put("name", "spu名字");
    
        JSONArray data1 = new JSONArray();
        for (Spu spu : list) {
            data1.add(spu.getOrder_amount());
        }
        
        one.put("data", data1);
    
        series.add(one);
    
        data.put("series", series);
    
    
        result.put("data", data);
    
    
        return result.toJSONString();
    }
    
}
