import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created by 張燿峰
 * 无状态转换操作案例
 * @author 孤
 * @date 2019/4/12
 * @Varsion 1.0
 */
public class StateLess {

    private static final Pattern SPACE = Pattern.compile(" ");

    static final class LogTuple implements PairFunction<String, String, Integer> {

        @Override
        public Tuple2<String, Integer> call(String s) {
            return new Tuple2<>(s, 1);
        }
    }

    static final class ReduceIsKey implements Function2<Integer, Integer, Integer> {

        @Override
        public Integer call(Integer v1, Integer v2) {
            return v1 + v2;
        }
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("StateLess");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<String> inputDStream = streamingContext.socketTextStream("localhost", 8080);

        JavaDStream<String> dStream = inputDStream.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairDStream<String, Integer> pairDStream = dStream.mapToPair(new LogTuple());

        JavaPairDStream<String, Integer> result = pairDStream.reduceByKey(new ReduceIsKey());

        //JOIN
        JavaPairDStream<String, Integer> pairDStream1 = dStream.mapToPair(new LogTuple());

        JavaPairDStream<String, Integer> result1 = pairDStream.reduceByKey(new ReduceIsKey());

        JavaPairDStream<String,Tuple2<Integer,Integer>> c = result.join(result);

        result.print();

    }
}
