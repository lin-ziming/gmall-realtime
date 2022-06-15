import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @Author lzc
 * @Date 2022/6/15 14:12
 */
public class Test2 {
    public static void main(String[] args) {
        JSONObject obj = new JSONObject();
        obj.put("a", 1);
        obj.put("b", 1);
        obj.put("c", 1);
        obj.put("e", 1);
        obj.put("f", 1);
    
    
        List<String> columns = Arrays.asList("a,b,c".split(","));
    
    
        Set<String> keys = obj.keySet();
        /*for (String key : keys) {  // 错误
            if (!columns.contains(key)) {
                keys.remove(key);
            }
        }*/
        /*Iterator<String> it = keys.iterator();
        while (it.hasNext()) {
            String key = it.next();
            if (!columns.contains(key)){
                it.remove();
            }
        }*/
    
        keys.removeIf(key -> !columns.contains(key));
    
        System.out.println(obj);
    }
}
