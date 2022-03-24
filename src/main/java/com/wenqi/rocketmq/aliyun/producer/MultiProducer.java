package com.wenqi.rocketmq.aliyun.producer;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;

import java.util.Date;
import java.util.Properties;

/**
 * @author liangwenqi
 * @date 2022/3/24
 */
public class MultiProducer {
    public static void main(String[] args) {
        // producer实例配置初始化。
        Properties properties = new Properties();
        // 您在消息队列RocketMQ版控制台创建的Group ID。
        properties.put(PropertyKeyConst.GROUP_ID, "XXX");
        // AccessKey ID阿里云身份验证，在阿里云服务器管理控制台创建。
        properties.put(PropertyKeyConst.AccessKey, "XXX");
        // AccessKey Secret阿里云身份验证，在阿里云服务器管理控制台创建。
        properties.put(PropertyKeyConst.SecretKey, "XXX");
        // 设置发送超时时间，单位：毫秒。
        properties.setProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        // 设置TCP接入域名，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看。
        properties.put(PropertyKeyConst.NAMESRV_ADDR, "XXX");
        final Producer producer = ONSFactory.createProducer(properties);
        // 在发送消息前，必须调用start方法来启动Producer，只需调用一次即可。
        producer.start();

        //创建的Producer和Consumer对象为线程安全的，可以在多线程间进行共享，避免每个线程创建一个实例。

        //在thread和anotherThread中共享Producer对象，并发地发送消息至消息队列RocketMQ版。
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Message msg = new Message(
                            // 普通消息所属的Topic，切勿使用普通消息的Topic来收发其他类型的消息。
                            "TopicTestMQ",
                            // Message Tag可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在消息队列RocketMQ版的服务器过滤。
                            "TagA",
                            // Message Body可以是任何二进制形式的数据，消息队列RocketMQ版不做任何干预。
                            // 需要Producer与Consumer协商好一致的序列化和反序列化方式。
                            "Hello MQ".getBytes());
                    SendResult sendResult = producer.send(msg);
                    // 同步发送消息，只要不抛异常就是成功。
                    if (sendResult != null) {
                        System.out.println(new Date() + " Send mq message success. Topic is:" + "XXX" + " msgId is: " + sendResult.getMessageId());
                    }
                } catch (Exception e) {
                    // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                    System.out.println(new Date() + " Send mq message failed. Topic is:" + "XXX");
                    e.printStackTrace();
                }
            }
        });
        thread.start();


        Thread anotherThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Message msg = new Message("TopicTestMQ", "TagA", "Hello MQ".getBytes());
                    SendResult sendResult = producer.send(msg);
                    // 同步发送消息，只要不抛异常就是成功。
                    if (sendResult != null) {
                        System.out.println(new Date() + " Send mq message success. Topic is:" + "XXX" + " msgId is: " + sendResult.getMessageId());
                    }
                } catch (Exception e) {
                    // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                    System.out.println(new Date() + " Send mq message failed. Topic is:" + "XXX");
                    e.printStackTrace();
                }
            }
        });
        anotherThread.start();


        //（可选）Producer实例若不再使用时，可将Producer关闭，进行资源释放。
         producer.shutdown();
    }
}
