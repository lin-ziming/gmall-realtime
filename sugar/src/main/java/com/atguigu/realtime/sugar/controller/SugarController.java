package com.atguigu.realtime.sugar.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2022/7/1 9:28
 */
@RestController
public class SugarController {
    @RequestMapping("/sugar/gmv")
    public String gmv(int date){
        
        return "ok";
    }
    
}
