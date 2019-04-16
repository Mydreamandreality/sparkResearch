import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Created by 張燿峰
 * reduceByKeyAndWindow的代码案例
 *
 * @author 孤
 * @date 2019/4/16
 * @Varsion 1.0
 */
public class ReduceByKeyAndWindow {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("reduceByKeyAndWindow").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        //检查点设置
        streamingContext.checkpoint("hdfs://localhost:9300");
        //数据源
        JavaDStream<String> dStream = streamingContext.socketTextStream("localhost", 8080);

        JavaPairDStream<String, Long> ipPairDstream = dStream.mapToPair(new GetIp());

        JavaPairDStream<String, Long> result = ipPairDstream.reduceByKeyAndWindow(new AddLongs(),
                new SubtractLongs(), Durations.seconds(30), Durations.seconds(10));

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class GetIp implements PairFunction<String, String, Long> {

        @Override
        public Tuple2<String, Long> call(String s) {
            return new Tuple2<>(s, 1L);
        }
    }

    /**
     * 加上新进入窗口的批次元素
     */
    static class AddLongs implements Function2<Long, Long, Long> {

        @Override
        public Long call(Long v1, Long v2) throws Exception {
            return v1 + v2;
        }
    }

    /**
     * 移除离开窗口的旧批次元素
     */
    static class SubtractLongs implements Function2<Long, Long, Long> {

        @Override
        public Long call(Long v1, Long v2) throws Exception {
            return v1 - v2;
        }
    }

}
