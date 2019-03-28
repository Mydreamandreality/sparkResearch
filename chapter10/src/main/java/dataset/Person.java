package dataset;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by 張燿峰
 *
 * @author 孤
 * @date 2019/3/28
 * @Varsion 1.0
 */
@Data
public class Person implements Serializable {
    private String name;
    private Integer age;

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }
}
