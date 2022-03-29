package com.wenqi.rocketmq.source.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author liangwenqi
 * @date 2022/3/28
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 实例化消息生产者Product
        DefaultMQProducer producer = new DefaultMQProducer("source-producer-quick-start");
        // 设置NameServer的地址
        producer.setNamesrvAddr("127.0.0.1:9876;127.0.0.1:19876");
        // 启动Producer实例
        producer.start();

        for (int i = 0; i < 100; i++) {
            try {
                // 创建消息, 并指定Topic, Tag和消息体
                byte[] messageBody = ("Source : Hello RocketMQ" + i).getBytes("UTF-8");
                Message message = new Message("TopicTest", "TagA", messageBody);
                // 发送消息到一个Broker
                // 通过sendResult返回消息是否成功送达
                SendResult sendResult = producer.send(message);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        // 如果不在发送消息, 关闭Producer实例
        producer.shutdown();
    }
}
