package com.wenqi.rocketmq.source.action.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 基于SQL表达式进行消息过滤，其实是对消息的属性运用SQL过滤表达式进行条件匹配，
 * 因此消息发送时应该调用putUserProperty方法设置消息属性
 *
 * @author Wenqi Liang
 * @date 2022/6/18
 */
public class SqlProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("Source-SqlProducer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message msg = new Message("TopicTest" /* Topic */, "TagA" /* Tag */, ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 使用SQL过滤, 需要设置property属性
            msg.putUserProperty("orderStatus", String.valueOf(i));
            msg.putUserProperty("sellerId", "21");
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
