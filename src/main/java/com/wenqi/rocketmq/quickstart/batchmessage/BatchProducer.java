package com.wenqi.rocketmq.quickstart.batchmessage;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liangwenqi
 * @date 2022/3/22
 */
public class BatchProducer {
    public static void main(String[] args) throws MQClientException {
        // 实例化消息生产者Product
        DefaultMQProducer producer = new DefaultMQProducer("batch-producer-demo-01");
        // 设置NameServer的地址
        producer.setNamesrvAddr("10.0.88.8:9876");
        // 启动Producer实例
        producer.start();

        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID003", "Hello world 2".getBytes()));

        //把大的消息分裂成若干个小的消息
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()) {
            try {
                List<Message>  listItem = splitter.next();
                producer.send(listItem);
            } catch (Exception e) {
                e.printStackTrace();
                //处理error
            }
        }

        // 如果不在发送消息, 关闭Producer实例
        producer.shutdown();
    }
}
