package CommonTools;

import javax.jms.*;

import Configure.MQParam;
import org.apache.activemq.*;
import org.apache.activemq.command.ActiveMQObjectMessage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static CommonTools.MQConnPool.getPoolConnection;
import static CommonTools.MQConnPool.pooledConnectionFactory;
import static CommonTools.MQConnPool.getConnection;

/**
 * 简单地封装了ActiveMQ生产者的操作
 *
 * @author ttimzxz
 * @version 1.0
 */

public class Producer {
    Connection connection = null;
    Session session;
    Destination destination;
    MessageProducer producer;

    public Producer(String queueName) {
        try {
            this.connection = getConnection();
            this.connection.start();
            this.session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            this.destination = session.createQueue(queueName);
            this.producer = session.createProducer(destination);
            this.producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取普通链接
    public Producer(String queueName, String type) {
        try {
            this.connection = getConnection();
            this.connection.start();
            this.session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            this.destination = session.createQueue(queueName);
            this.producer = session.createProducer(destination);
            this.producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (null != this.connection) {
            try {
                this.connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    public void sendMessage(String message) {
        try {
            TextMessage Text = this.session.createTextMessage(message);
            this.producer.send(Text);
            this.session.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * @param time    等待时间（单位：ms）
     * @param message 待发送消息
     */
    //延时发送消息，time参数单位为ms
    public void sendMessage(long time, String message) {
        try {
            TextMessage Text = this.session.createTextMessage(message);
            Text.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
            this.producer.send(Text);
            this.session.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendObject(Object message) {
        try {
            ActiveMQObjectMessage msg = (ActiveMQObjectMessage) session.createObjectMessage();
            msg.setObject((Serializable) message);
            producer.send(msg);
            session.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
