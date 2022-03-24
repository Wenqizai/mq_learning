package com.wenqi.rocketmq.aliyun.producer;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.OnExceptionContext;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendCallback;
import com.aliyun.openservices.ons.api.SendResult;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 异步消息
 *
 * @author liangwenqi
 * @date 2022/3/24
 */
public class AsyncProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // AccessKeyId 阿里云身份验证，在阿里云服务器管理控制台创建。
        properties.put(PropertyKeyConst.AccessKey, "XXX");
        // AccessKeySecret 阿里云身份验证，在阿里云服务器管理控制台创建。
        properties.put(PropertyKeyConst.SecretKey, "XXX");
        //设置发送超时时间，单位毫秒。
        properties.setProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        // 设置TCP接入域名，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看。
        properties.put(PropertyKeyConst.NAMESRV_ADDR, "XXX");

        Producer producer = ONSFactory.createProducer(properties);
        // 在发送消息前，必须调用start方法来启动Producer，只需调用一次即可。
        producer.start();

        Message msg = new Message(
                // 普通消息所属的Topic，切勿使用普通消息的Topic来收发其他类型的消息。
                "TopicTestMQ",
                // Message Tag，可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在消息队列RocketMQ版的服务器过滤。
                "TagA",
                // Message Body，任何二进制形式的数据，消息队列RocketMQ版不做任何干预，需要Producer与Consumer协商好一致的序列化和反序列化方式。
                "Hello MQ".getBytes());

        // 设置代表消息的业务关键属性，请尽可能全局唯一。 以方便您在无法正常收到消息情况下，可通过消息队列RocketMQ版控制台查询消息并补发。
        // 注意：不设置也不会影响消息正常收发。
        msg.setKey("ORDERID_100");

        // 异步发送消息, 发送结果通过callback返回给客户端。
        producer.sendAsync(msg, new SendCallback() {
            @Override
            public void onSuccess(final SendResult sendResult) {
                // 消息发送成功。
                System.out.println("send message success. topic=" + sendResult.getTopic() + ", msgId=" + sendResult.getMessageId());
            }

            @Override
            public void onException(OnExceptionContext context) {
                // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                System.out.println("send message failed. topic=" + context.getTopic() + ", msgId=" + context.getMessageId());
            }
        });

        // 阻塞当前线程3秒，等待异步发送结果。
        TimeUnit.SECONDS.sleep(3);

        // 在应用退出前，销毁Producer对象。注意：如果不销毁也没有问题。
        producer.shutdown();
    }
}
