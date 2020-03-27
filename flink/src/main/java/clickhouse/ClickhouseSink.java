package clickhouse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: clickhouse
 * @Author: luk
 * @CreateTime: 2020/3/27 14:16
 */
public class ClickhouseSink extends RichSinkFunction<UserBean> {
    private String address = "jdbc:clickhouse://node03:8123/default";
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        connection = DriverManager.getConnection(address);
    }

    @Override
    public void invoke(UserBean userBean, Context context) throws Exception {
        int id = userBean.getId();
        String name = userBean.getName();
        int age = userBean.getAge();
        String data_date = userBean.getData_date();
        PreparedStatement statement = connection.prepareStatement("insert into ods_user_login (id, name, age, data_date) " +
                "values(?,?,?,?)");
        statement.setInt(1, id);
        statement.setString(2, name);
        statement.setInt(3, age);
        statement.setString(4, data_date);

        statement.execute();
    }

    @Override
    public void close() throws Exception {

    }
}
