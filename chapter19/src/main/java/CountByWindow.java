import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by 張燿峰
 * countByWindow
 *
 * @author 孤
 * @date 2019/4/16
 * @Varsion 1.0
 */
public class CountByWindow {

    public static void countWindow(JavaDStream<String> javaDStream) {
        JavaDStream ip = javaDStream.map((Function<String, Object>) v1 -> v1);

        JavaDStream<Long> ipCount = ip.countByWindow(Durations.seconds(30), Durations.seconds(10));
        JavaPairDStream<String, Long> ipAddressRequestCount = ip.countByValueAndWindow(Durations.seconds(30), Durations.seconds(10));
    }

}
