import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.jumpmind.symmetric.csv.CsvReader;

import java.io.StringReader;


/**
 * Created by 張燿峰
 * CSV文件操作
 * @author 孤
 * @date 2019/3/22
 * @Varsion 1.0
 */
public class CsvFile {

    static class readCsv implements Function<String,String[]>{
        @Override
        public String[] call(String v1) throws Exception {
            CsvReader reader = new CsvReader(new StringReader(v1));
            return reader.getValues();
        }
    }

    protected static void run(JavaSparkContext sparkContext){
        JavaRDD<String> csvFile1 = sparkContext.textFile("url");
        JavaRDD<String[]> csvData = csvFile1.map(new readCsv());
    }
}
