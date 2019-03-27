import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import sun.plugin2.message.ShowDocumentMessage;

import static org.apache.spark.sql.functions.col;
/**
 * Created by 張燿峰
 * sparkSQL测试案例
 * @author 孤
 * @date 2019/3/27
 * @Varsion 1.0
 */
public class SparkSqlApplication {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local")
                .appName("Java Spark SQL")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read().json("URL");
        //只返回name字段
        dataset.select("name").show();
        //返回两个字段,所有age的value+1
        dataset.select(col("name"),col("age").plus(1)).show();
        //选择age大于21岁的人
        dataset.filter(col("age").gt(21)).show();
        //分组聚合,group age
        dataset.groupBy("age").count().show();
        //显示
        dataset.show();


        /*以编程的方式运行SQL查询*/
        //注册临时表
        dataset.createOrReplaceGlobalTempView("user");
        Dataset<Row> users = sparkSession.sql("SELECT * FROM user");
        users.show();
    }
}
