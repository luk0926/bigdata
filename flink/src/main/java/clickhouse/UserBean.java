package clickhouse;

import java.io.Serializable;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: clickhouse
 * @Author: luk
 * @CreateTime: 2020/3/27 14:31
 */
public class UserBean implements Serializable {
    private int id;
    private String name;
    private int age;
    private String data_date;

    public UserBean() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    @Override
    public String toString() {
        return "UserBean{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", data_date='" + data_date + '\'' +
                '}';
    }
}
