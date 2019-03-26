import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 張燿峰
 * 累加器
 * @author 孤
 * @date 2019/3/25
 * @Varsion 1.0
 */
public class AttackAccumulator extends AccumulatorV2<String, String> {

    /*定义我需要计算的变量*/
    private Integer emptyLine;

    /*定义变量的初始值*/
    private String initEmptyLine = JavaBean.origin_id + ":0;" + JavaBean.asset_name + ":0;";

    /*初始化原始状态*/
    private String resetInitEmptyLine = initEmptyLine;

    /*判断是否等于初始状态*/
    @Override
    public boolean isZero() {
        return initEmptyLine.equals(resetInitEmptyLine);
    }

    //复制新的累加器
    @Override
    public AccumulatorV2<String, String> copy() {
        return new AttackAccumulator();
    }

    /*初始化原始状态*/
    @Override
    public void reset() {
        initEmptyLine = resetInitEmptyLine;
    }

    /*针对传入的新值,与当前累加器已有的值进行累加*/
    @Override
    public void add(String s) {
        initEmptyLine = mergeData(s,initEmptyLine,";");
    }

    /*将两个累加器的计算结果合并*/
    @Override
    public void merge(AccumulatorV2<String, String> accumulatorV2) {
        initEmptyLine = mergeData(accumulatorV2.value(), initEmptyLine, ";");
    }

    /*返回累加器的值*/
    @Override
    public String value() {
        return initEmptyLine;
    }

    /*筛选字段为null的*/
    private Integer mergeEmptyLine(String key, String value, String delimit) {
        return 0;
    }


    private static String mergeData(String data_1, String data_2, String delimit) {
        StringBuffer stringBuffer = new StringBuffer();
        //通过分割的方式获取value
        String[] info_1 = data_1.split(delimit);
        String[] info_2 = data_2.split(delimit);

        //处理info_1数据
        Map<String, Integer> mapNode = resultKV(":", info_1);

        //处理info_2数据
        Map<String, Integer> mapNodeTo = resultKV(":", info_2);

        consoleResult(delimit, stringBuffer, mapNodeTo, mapNode);

        consoleResult(delimit, stringBuffer, mapNode, mapNodeTo);

        return stringBuffer.toString().substring(0, stringBuffer.toString().length() - 1);
    }

    private static void consoleResult(String delimit, StringBuffer stringBuffer, Map<String, Integer> mapNode, Map<String, Integer> mapNodeTo) {
        for (Map.Entry<String, Integer> entry : mapNodeTo.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            if (value == null || 0 == (value)) {
                value += 1;
                if (mapNode.containsKey(key) && (mapNode.get(key) == null || 0 == (mapNode.get(key)))) {
                    value += 1;
                    mapNode.remove(key);
                    stringBuffer.append(key + ":" + value + delimit);
                    continue;
                }
                stringBuffer.append(key + ":" + value + delimit);
            }
        }
    }


    private static Map<String, Integer> resultKV(String delimit, String[] infos) {
        Map<String, Integer> mapNode = new HashMap<>();
        for (String info : infos) {
            String[] kv = info.split(delimit);
            if (kv.length == 2) {
                String k = kv[0];
                Integer v = Integer.valueOf(kv[1]);
                mapNode.put(k, v);
                continue;
            }
        }
        return mapNode;
    }
}
