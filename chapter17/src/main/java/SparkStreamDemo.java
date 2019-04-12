import javafx.util.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by 張燿峰
 * sparkStreaming入门案例
 *
 * @author 孤
 * @date 2019/4/11
 * @Varsion 1.0
 */
public class SparkStreamDemo {

    public static void main(String[] args) {
        //创建两个核心的本地线程,批处理的间隔为1秒
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("sparkStreamIng");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
        //创建一个连接到IP:localhost,PORT:8080的DStream
        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 8080);
        JavaDStream<String> errorLine = dStream.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("error");
            }
        });
        //打印包含error的行
        errorLine.print();
        try {
            //开始计算
            javaStreamingContext.start();
            //等待计算完成
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
