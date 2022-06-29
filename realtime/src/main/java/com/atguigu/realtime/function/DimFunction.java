package com.atguigu.realtime.function;

import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    String getTable();
    
    String getId(T input);
    
    void addDim(T input,
                JSONObject dim);
    
    
}
