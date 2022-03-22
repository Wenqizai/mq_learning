package com.wenqi.rocketmq.quickstart.usetag;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author liangwenqi
 * @date 2022/3/22
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Product
        DefaultMQProducer producer = new DefaultMQProducer("producer-tag-demo-01");
        // 设置NameServer的地址
        producer.setNamesrvAddr("10.0.88.8:9876");
        // 启动Producer实例
        producer.start();

        for (int i = 0; i < 100; i++) {
            // 创建消息, 并指定Topic, Tag和消息体
            byte[] messageBody = ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message message = new Message("TopicTest", "TagA", messageBody);
            // 设置一些属性
            message.putUserProperty("a", String.valueOf(i));
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(message);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }

        // 如果不在发送消息, 关闭Producer实例
        producer.shutdown();
    }
}
