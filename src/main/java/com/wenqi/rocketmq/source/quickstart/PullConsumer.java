package com.wenqi.rocketmq.source.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author liangwenqi
 * @date 2022/3/28
 */
public class PullConsumer {
    private final static Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<>();

    public static void main(String[] args) throws MQClientException {
        // 实例化消费者
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("source-consumer-quick-start");

        // 设置NameServer地址
        consumer.setNamesrvAddr("127.0.0.1:9876;127.0.0.1:19876");
        consumer.start();

        // 订阅一个或者多个Topic, 以及Tag来过滤需要消费的消息
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            long Offset = consumer.fetchConsumeOffset(mq, true);
            System.out.printf(" Consume from the Queue: " + mq + "%n");
            while (true) {
                try {
                    long startTime = System.currentTimeMillis();
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    long endTime = System.currentTimeMillis();
                    System.out.printf("pullResult = %s costTime = %s秒 %n", pullResult, (endTime - startTime) / 1000);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            System.out.printf("FOUND %s %n", pullResult);
                            List<String> messageContents = pullResult.getMsgFoundList().stream().map(item -> new String(item.getBody())).collect(Collectors.toList());
                            System.out.printf("messageContents = %s %n", messageContents);
                            break;
                        case NO_MATCHED_MSG:
                            System.out.printf("NO_MATCHED_MSG %s %n", pullResult);
                            break;
                        case NO_NEW_MSG:
                            System.out.printf("NO_NEW_MSG %s %n", pullResult);
                            break;
                        case OFFSET_ILLEGAL:
                            System.out.printf("OFFSET_ILLEGAL %s %n", pullResult);
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long Offset = OFFSET_TABLE.get(mq);
        if (Offset != null) {
            return Offset;
        }
        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long Offset) {
        OFFSET_TABLE.put(mq, Offset);
    }
}
