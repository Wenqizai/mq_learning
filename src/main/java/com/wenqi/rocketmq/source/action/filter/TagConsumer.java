package com.wenqi.rocketmq.source.action.filter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author Wenqi Liang
 * @date 2022/6/18
 */
public class TagConsumer {
    public static void main(String[] args) throws Exception {
        // 订单系统消费组
        DefaultMQPushConsumer orderConsumer = new DefaultMQPushConsumer("Source-tag-order-Consumer");
        orderConsumer.setNamesrvAddr("127.0.0.1:9876");
        orderConsumer.setMessageModel(MessageModel.CLUSTERING);
        // 订阅TOPIC_TAG_ALL和TOPIC_TAG_ORDER
        orderConsumer.subscribe("TopicFilter", "TOPIC_TAG_ALL || TOPIC_TAG_ORDER");
        orderConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("%s orderConsumer Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 库存系统消费组
        DefaultMQPushConsumer stockConsumer = new DefaultMQPushConsumer("Source-tag-stock-Consumer");
        stockConsumer.setNamesrvAddr("127.0.0.1:9876");
        stockConsumer.setMessageModel(MessageModel.CLUSTERING);
        // 订阅TOPIC_TAG_ALL和TOPIC_TAG_STOCK
        stockConsumer.subscribe("TopicFilter", "TOPIC_TAG_ALL || TOPIC_TAG_STOCK");
        stockConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("%s stockConsumer Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        orderConsumer.start();
        stockConsumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
