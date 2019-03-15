package spark;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by 張燿峰
 * Lombda 语法测试
 * @author 孤
 * @date 2019/3/13
 * @Varsion 1.0
 */
public class Lombda {
    public static void main(String[] args) {
        returnResult();
    }

    public void comparator() {

    }

    public void map() {
        String[] players = {"Rafael Nadal", "Novak Djokovic",
                "Stanislas Wawrinka", "David Ferrer",
                "Roger Federer", "Andy Murray",
                "Tomas Berdych", "Juan Martin Del Potro",
                "Richard Gasquet", "John Isner"};

// 1.1 使用匿名内部类根据 name 排序 players
        Arrays.sort(players, new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return (s1.compareTo(s2));
            }
        });

        Arrays.asList(players).stream().forEach(x -> System.out.println(x));
    }


    public static void returnResult() {
        List<String> proNames = Arrays.asList(new String[]{"Test","Test1312","test2"});
        List<String> lowerProNames = proNames.stream().map(name->name.toLowerCase()).collect(Collectors.toList());
        lowerProNames.forEach(System.out::println);

    }

}
