import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by 張燿峰
 * inner join 查询
 * @author 孤
 * @date 2019/4/16
 * @Varsion 1.0
 */
public class Join {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("StateLess");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<String> inputDStream = streamingContext.socketTextStream("localhost", 8080);
        JavaReceiverInputDStream<String> inputDStream1 = streamingContext.socketTextStream("localhost", 8081);

    }
}
