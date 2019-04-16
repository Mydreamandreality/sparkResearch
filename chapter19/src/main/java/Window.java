import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by 張燿峰
 * 窗口转换操作
 *
 * @author 孤
 * @date 2019/4/15
 * @Varsion 1.0
 */
public class Window {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("window").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        //检查点设置
        streamingContext.checkpoint("hdfs://localhost:9300");

        JavaDStream<String> dStream = streamingContext.socketTextStream("localhost", 8080);

        JavaDStream<String> winDstream = dStream.window(Durations.seconds(30), Durations.seconds(20));

        JavaDStream<Long> result = winDstream.count();

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
