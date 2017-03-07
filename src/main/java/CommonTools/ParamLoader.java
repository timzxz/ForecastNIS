package CommonTools;

/**
 * 载入basic.properties
 * <p>
 * TODO: 将Configure模块中的参数移至properties文件中，修改相应的使用参数的代码
 */

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ParamLoader {
    public static Properties param = getProperties();

    private static Properties getProperties() {
        String path = System.getProperty("user.dir");
//        System.out.println(path);
        String confPath = path.concat(File.separator).concat("basic.properties");
        Properties prop = new Properties();
        try {
            //读取属性文件a.properties
            InputStream in = new BufferedInputStream(new FileInputStream(confPath));
            prop.load(in);     ///加载属性列表
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return prop;

    }

    public static void main(String[] args) {

        Properties param = getProperties();
        // System.out.println(param.getProperty("s"));
        System.out.println(param.getProperty("hbase.zookeeper.quorum"));
    }
}
