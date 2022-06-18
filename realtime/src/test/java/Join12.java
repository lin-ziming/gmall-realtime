import com.atguigu.realtime.app.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/18 14:15
 */
public class Join12 extends BaseAppV1 {
    public static void main(String[] args) {
        new Join12().init(10000,2,"Join13", "s12");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
