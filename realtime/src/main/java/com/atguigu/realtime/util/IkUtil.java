package com.atguigu.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/24 10:10
 */
public class IkUtil {
    // 使用ik分词器把传入的字符串进行分词
    //TODO
    public static List<String> split(String keyword) {
        List<String> result = new ArrayList<>();
        // string ->  reader ?
        
        // 内存流  StringReader
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
    
        Lexeme next = null;
        try {
            next = ikSegmenter.next();
            while (next != null) {
                String kw = next.getLexemeText();
                result.add(kw);
        
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
        // list集合中可能有重复元素,实现去重
        HashSet<String> set = new HashSet<>(result);
        result.clear();
        result.addAll(set);
    
        return result;
    }
    
    public static void main(String[] args) {
        System.out.println(split("手机 华为手机 华为256g手机"));
    }
}
