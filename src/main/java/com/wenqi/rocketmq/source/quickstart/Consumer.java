package com.wenqi.rocketmq.source.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * @author liangwenqi
 * @date 2022/3/28
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("source-consumer-quick-start");

        // 设置NameServer地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //consumer.setNamesrvAddr("127.0.0.1:9876;127.0.0.1:19876");

        // 订阅一个或者多个Topic, 以及Tag来过滤需要消费的消息
        consumer.subscribe("TopicTest", "*");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            // MessageListenerConcurrently, MessageListenerOrderly 继承 MessageListener
            // MessageListenerConcurrently: 并发消费, MessageListenerOrderly: 顺序消费
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            // 标记该消息已经被成功消费
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });

        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
