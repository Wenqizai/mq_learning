package com.wenqi.rocketmq.quickstart.producer;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author liangwenqi
 * @date 2022/3/21
 */
public class OneWayProducer {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Product
        DefaultMQProducer producer = new DefaultMQProducer("one-way-producer-demo-01");
        // 设置NameServer的地址
        producer.setNamesrvAddr("10.0.88.8:9876");
        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 100; i++) {
            // 创建消息, 并指定Topic, Tag和消息体
            Message msg = new Message("TopicTest",
                    "TagA",
                    ("Hello RocketMQ : oneWay" + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 发送单向消息，没有任何返回结果
            producer.sendOneway(msg);

        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
