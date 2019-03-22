import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by 張燿峰
 * SeqnenceFile操作案例
 *
 * @author 孤
 * @date 2019/3/22
 * @Varsion 1.0
 */
public class SequenceFile {

    protected static void run(JavaSparkContext sparkContext) {
        JavaPairRDD<Text, IntWritable> javaPairRDD = sparkContext.sequenceFile("url", Text.class, IntWritable.class);
        JavaPairRDD<String, Integer> pairRDD = javaPairRDD.mapToPair(new sequenceToConvert());
        //写
        pairRDD.saveAsHadoopFile("url",Text.class,IntWritable.class,SequenceFileOutputFormat.class);
    }


    static class sequenceToConvert implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {

        @Override
        public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> textIntWritableTuple2) {
            return new Tuple2<>(textIntWritableTuple2._1.toString(), textIntWritableTuple2._2.get());
        }
    }

}
