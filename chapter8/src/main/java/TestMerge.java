import lombok.extern.slf4j.Slf4j;
import scala.Int;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 張燿峰
 * 测试mergeData
 * @author 孤
 * @date 2019/3/25
 * @Varsion 1.0
 */
@Slf4j
public class TestMerge {

    public static void main(String[] args) {
        String testMerge = mergeData("origin_id:99", "asset_name:100", ";");
        log.info(testMerge);
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

        return stringBuffer.toString();
    }

    private static void consoleResult(String delimit, StringBuffer stringBuffer, Map<String, Integer> mapNode, Map<String, Integer> mapNodeTo) {
        for (Map.Entry<String, Integer> entry : mapNodeTo.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            if (value == null || 0 == (value)) {
                value +=1;
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
