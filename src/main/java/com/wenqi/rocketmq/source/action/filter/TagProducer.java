package com.wenqi.rocketmq.source.action.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author Wenqi Liang
 * @date 2022/6/18
 */
public class TagProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("Source-TagProducer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                String body = "HelloWorld" + i;
                Message msg = new Message("TopicFilter", "TOPIC_TAG_ALL", "OrderID001", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                System.out.printf("%s%n", producer.send(msg));
            } else {
                String body = "Hello world" + i;
                Message msg = new Message("TopicFilter", "TOPIC_TAG_ORDER", "OrderID001", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                System.out.printf("%s%n", producer.send(msg));
            }
        }
        producer.shutdown();
    }
}
