package CommonTools;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;

import static CommonTools.ParamLoader.param;

public class MQConnPool {
    private static ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
            ActiveMQConnection.DEFAULT_USER,
            ActiveMQConnection.DEFAULT_PASSWORD,
            param.getProperty("MQConnectURL"));

    public static PooledConnectionFactory pooledConnectionFactory = getPooledConnectionFactory();

    //初始化连接池
    private static PooledConnectionFactory getPooledConnectionFactory() {
        try {
            pooledConnectionFactory = new PooledConnectionFactory(connectionFactory);
            //设置最大连接数
            pooledConnectionFactory.setMaxConnections(1000);
            return pooledConnectionFactory;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //获取普通连接
    public static Connection getConnection() throws JMSException {
        //当delivery模式为持久模式时，可以设置sender等待发送成功回执为异步模式，并且设置回执窗口
        connectionFactory.setUseAsyncSend(true);
        connectionFactory.setProducerWindowSize(102400);

        return connectionFactory.createConnection();
    }

    //获取连接池连接
    public static Connection getPoolConnection() throws JMSException {

        return pooledConnectionFactory.createConnection();
    }

    /**
     * 对象回收销毁时停止链接
     */
    @Override
    protected void finalize() throws Throwable {
        pooledConnectionFactory.stop();
        super.finalize();
    }
}
