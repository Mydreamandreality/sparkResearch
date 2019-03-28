import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by 張燿峰
 * 创建DataSet
 *
 * @author 孤
 * @date 2019/3/28
 * @Varsion 1.0
 */
public class global {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local")
                .appName("Java Spark SQL")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read().json("URL");
        try {
            //创建全局临时视图
            dataset.createGlobalTempView("user");
            //全局临时视图绑定到系统保存的数据库“global_temp”
            Dataset<Row> globalUser = sparkSession.sql("SELECT * FROM global_temp.user");
            sparkSession.newSession().sql("SELECT * FROM global_temp.user");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
