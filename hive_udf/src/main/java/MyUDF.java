import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: PACKAGE_NAME
 * @Author: luk
 * @CreateTime: 2019/12/20 10:45
 */
public class MyUDF extends UDF {

    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }

        //返回大写字母
        return new Text(s.toString().toUpperCase());
    }
}
