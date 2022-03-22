package com.wenqi.rocketmq.quickstart.usetag;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

/**
 * @author liangwenqi
 * @date 2022/3/22
 */
public class Consumer {
    public static void main(String[] args) throws Exception {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_tag_demo_01");

        // 设置NameServer地址
        consumer.setNamesrvAddr("10.0.88.8:9876");

        // 只有订阅的消息有这个属性a, a >=0 and a <= 3
        // 只有使用push模式的消费者才能用使用SQL92标准的sql语句
        consumer.subscribe("TopicTest", MessageSelector.bySql("a between 0 and 3"));

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
    }
}
