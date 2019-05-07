package v1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.mllib.feature.HashingTF;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by 張燿峰
 * 机器学习入门案例
 * 过滤垃圾邮件
 * 这 个 程 序 使 用 了 MLlib 中 的 两 个 函 数：HashingTF 与
 * LogisticRegressionWithSGD，前者从文本数据构建词频（term frequency）特征向量，后者
 * 使用随机梯度下降法（Stochastic Gradient Descent，简称 SGD）实现逻辑回归。假设我们
 * 从两个文件 spam.txt 与 normal.txt 开始，两个文件分别包含垃圾邮件和非垃圾邮件的例子，
 * 每行一个。接下来我们就根据词频把每个文件中的文本转化为特征向量，然后训练出一个 可以把两类消息分开的逻辑回归模型
 * @author 孤
 * @date 2019/5/7
 * @Varsion 1.0
 */
public class SpamEmail {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("spam-email").master("local[2]").getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        //垃圾邮件数据
        JavaRDD<String> spamEmail = javaSparkContext.textFile("spam.json");
        //优质邮件数据
        JavaRDD<String> normalEmail = javaSparkContext.textFile("normal.json");

        //创建hashingTF实例把邮件文本映射为包含10000个特征的向量
        final HashingTF hashingTF = new HashingTF(10000);

        JavaRDD<LabeledPoint> spamExamples = spamEmail.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String v1) throws Exception {
                return new LabeledPoint(0, hashingTF.transform(Arrays.asList(SPACE.split(v1))));
            }
        });

        JavaRDD<LabeledPoint> normaExamples = normalEmail.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String v1) throws Exception {
                return new LabeledPoint(1, hashingTF.transform(Arrays.asList(SPACE.split(v1))));
            }
        });

        //训练数据
        JavaRDD<LabeledPoint> trainData = spamExamples.union(normaExamples);
        trainData.cache();      //逻辑回归需要迭代,先缓存

        //随机梯度下降法  SGD 逻辑回归
        LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());

        Vector spamModel = hashingTF.transform(Arrays.asList(SPACE.split("垃 圾 钱 恶 心 色 情 赌 博 毒 品 败 类 犯罪")));
        Vector normaModel = hashingTF.transform(Arrays.asList(SPACE.split("work 工作 你好 我们 请问 时间 领导")));
        System.out.println("预测负面的例子: " + model.predict(spamModel));
        System.out.println("预测积极的例子: " + model.predict(normaModel));

    }
}
