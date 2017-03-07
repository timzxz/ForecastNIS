package CommonTools;

import javax.jms.*;

import Configure.MQParam;
import org.apache.activemq.*;

import static CommonTools.ParamLoader.param;
import static CommonTools.MQConnPool.pooledConnectionFactory;
import static CommonTools.MQConnPool.getPoolConnection;
import static CommonTools.MQConnPool.getConnection;

/**
 * 简单地封装了ActiveMQ消费者的操作
 *
 * @author ttimzxz
 * @version 1.0
 */
public class Consumer {
    Connection connection = null;
    Session session;
    Destination destination;
    public MessageConsumer consumer;

    public Consumer(String queueName) {
        try {
            connection = getConnection();
            connection.start();
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue(queueName);
            consumer = session.createConsumer(destination);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (null != connection)
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
    }

    public String recieve() {
        String message = null;
        TextMessage textMessage = null;
        try {
            textMessage = (TextMessage) consumer.receive();
            if (null != textMessage) {
                message = textMessage.getText().toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }
}
