import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by 張燿峰
 * 获取JDBC数据源
 * @author 孤
 * @date 2019/4/3
 * @Varsion 1.0
 */
public class LoadJdbc {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("loadJDBC")
                .master("local")
                .getOrCreate();

        //第一种方式
        Dataset<Row> rddDataset = sparkSession.read().format("jdbc")
                .option("url","jdbc:mysql:dbserver")
                .option("user","root")
                .option("password","root")
                .option("dbtable","sys_alarm")
                .load();

        //创建的第二种方式
        Properties properties = new Properties();
        properties.put("user","root");
        properties.put("password","root");
        Dataset<Row> rowDataset = sparkSession.read().jdbc("jdbc:mysql:dbserver","sys_alarm",properties);

        //数据保存到JDBC源
        rddDataset.write().format("jdbc")
                .option("url","jdbc:mysql:dbserver")
                .option("user","root")
                .option("password","root")
                .option("dbtable","sys_alarm")
                .save();

        //第二种方式把数据保存到JDBC源
        rddDataset.write()
                .jdbc("jdbc:mysql:dbserver","sys_alarm",properties);

        //第三种方式:指定写的时候创建表列的数据类型
        rddDataset.write()
                .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
                .jdbc("jdbc:mysql:dbserver","sys_alarm",properties);
    }

}
