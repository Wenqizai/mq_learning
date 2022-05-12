package com.wenqi.rocketmq.quickstart.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @author liangwenqi
 * @date 2022/3/22
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group_demo_01");

        // 设置NameServer地址
        consumer.setNamesrvAddr("10.0.88.8:9876");

        // 订阅一个或者多个Topic, 以及Tag来过滤需要消费的消息
        consumer.subscribe("TopicTest", "*");

        // 设置广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 设置集群模式消费
        consumer.setMessageModel(MessageModel.CLUSTERING);

        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            // MessageListenerConcurrently, MessageListenerOrderly 继承 MessageListener
            // MessageListenerConcurrently: 并发消费, MessageListenerOrderly: 顺序消费
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            // 标记该消息已经被成功消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
