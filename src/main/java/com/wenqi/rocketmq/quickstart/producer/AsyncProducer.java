package com.wenqi.rocketmq.quickstart.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 发送异步消息
 * @author liangwenqi
 * @date 2022/3/21
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Product
        DefaultMQProducer producer = new DefaultMQProducer("async-producer-demo-01");
        // 设置NameServer的地址
        producer.setNamesrvAddr("10.0.88.8:9876");
        // 启动Producer实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i = 0; i < 100; i++) {
            final int index = i;
            // 创建消息, 并指定Topic, Tag和消息体
            byte[] messageBody = ("Hello world").getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message message = new Message("TopicTest", "TagA", "OrderID188", messageBody);
            // SendCallback接收异步返回结果的回调
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }

        Thread.sleep(3000);

        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
