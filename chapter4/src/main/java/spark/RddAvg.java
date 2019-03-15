package spark;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by 張燿峰
 * aggregate计算AVG
 *
 * @author 孤
 * @date 2019/3/15
 * @Varsion 1.0
 */
public class RddAvg {
    private Integer total;
    private Integer num;

    public RddAvg(Integer total, Integer num) {
        this.total = total;
        this.num = num;
    }

    public double avg() {
        return total / num;
    }

    Function2<RddAvg, Integer, RddAvg> avgFunction2 = new Function2<RddAvg, Integer, RddAvg>() {
        @Override
        public RddAvg call(RddAvg v1, Integer v2) {
            v1.total += v2;
            v1.num += 1;
            return v1;
        }
    };

    Function2<RddAvg,RddAvg,RddAvg> rddAvgFunction2 = new Function2<RddAvg, RddAvg, RddAvg>() {
        @Override
        public RddAvg call(RddAvg v1, RddAvg v2) {
            v1.total += v2.total;
            v1.num += v2.num;
            return v1;
        }
    };
}
