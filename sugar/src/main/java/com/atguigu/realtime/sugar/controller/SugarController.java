package com.atguigu.realtime.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.sugar.bean.Province;
import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.bean.Tm;
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
    public String gmv(int date) {
        
        Double gmv = tradeService.gmv(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", gmv);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/gmv/spu")
    public String gmvBySpu(int date) {
        
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
    
    
    @RequestMapping("/sugar/gmv/tm")
    public String gmvByTm(int date) {
        
        List<Tm> list = tradeService.gmvByTm(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONArray data = new JSONArray();
        
        for (Tm tm : list) {
            JSONObject obj = new JSONObject();
            
            obj.put("name", tm.getTrademark_name());
            obj.put("value", tm.getOrder_amount());
            
            data.add(obj);
        }
        
        
        result.put("data", data);
        
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/province")
    public String province(int date) {
    
        List<Province> list = tradeService.statsByProvince(date);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONObject data = new JSONObject();
        
        JSONArray mapData = new JSONArray();
    
        for (Province province : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", province.getProvince_name());
            obj.put("value", province.getOrder_amount());
    
            JSONArray tooltipValues = new JSONArray();
            tooltipValues.add(province.getOrder_count());
            obj.put("tooltipValues", tooltipValues);
            
            mapData.add(obj);
        }
        
        
        data.put("mapData", mapData);
        
        data.put("valueName", "订单金额");
        
        JSONArray tooltipNames = new JSONArray();
        tooltipNames.add("订单数");
        data.put("tooltipNames", tooltipNames);
        
        
        JSONArray tooltipUnits = new JSONArray();
        tooltipUnits.add("个");
        data.put("tooltipUnits", tooltipUnits);
        
        result.put("data", data);
        
        
        return result.toJSONString();
    }
    
}
