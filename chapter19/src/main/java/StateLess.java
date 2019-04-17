import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import tools.ConnectionPool;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by 張燿峰
 * 无状态转换操作案例
 *
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

        JavaPairDStream<String, Tuple2<Integer, Integer>> c = result.join(result);


        result.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionOfRecords -> {
                Connection connection = ConnectionPool.getConnection();
                Tuple2<String, Integer> wordCount;
                while (partitionOfRecords.hasNext()) {
                    wordCount = partitionOfRecords.next();
                    String sql = "insert into wordcount(word,count) " + "values('" + wordCount._1 + "',"
                            + wordCount._2 + ")";
                    Statement stmt = connection.createStatement();
                    stmt.executeUpdate(sql);
                }
                ConnectionPool.returnConnection(connection);
            });
        });

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
            streamingContext.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    class outFormat extends SequenceFileOutputFormat<Text, LongWritable> {
    }
}
