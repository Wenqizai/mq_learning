package com.wenqi.rocketmq.aliyun.normalmsg.producer;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;

import java.util.Properties;

/**
 * 单向消息: 发送方只负责发送消息, 不等待服务端返回响应且没有回调函数触发, 即只发送请求不等待应答
 *
 * @author liangwenqi
 * @date 2022/3/24
 */
public class OneWayProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // AccessKeyId 阿里云身份验证，在阿里云用户信息管理控制台创建。
        properties.put(PropertyKeyConst.AccessKey, "XXX");
        // AccessKeySecret 阿里云身份验证，在阿里云用户信息管理控制台创建。
        properties.put(PropertyKeyConst.SecretKey, "XXX");
        //设置发送超时时间，单位：毫秒。
        properties.setProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        // 设置TCP接入域名，进入消息队列RocketMQ版控制台的实例详情页面的接入点区域查看。
        properties.put(PropertyKeyConst.NAMESRV_ADDR,
                "XXX");

        Producer producer = ONSFactory.createProducer(properties);
        // 在发送消息前，必须调用start方法来启动Producer，只需调用一次即可。
        producer.start();
        //循环发送消息。
        for (int i = 0; i < 100; i++){
            Message msg = new Message(
                    // 普通消息所属的Topic，切勿使用普通消息的Topic来收发其他类型的消息。
                    "TopicTestMQ",
                    // Message Tag,
                    // 可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在消息队列RocketMQ版的服务器过滤。
                    "TagA",
                    // Message Body
                    // 任何二进制形式的数据，消息队列RocketMQ版不做任何干预，需要Producer与Consumer协商好一致的序列化和反序列化方式。
                    "Hello MQ".getBytes());

            // 设置代表消息的业务关键属性，请尽可能全局唯一。
            // 以方便您在无法正常收到消息情况下，可通过消息队列RocketMQ版控制台查询消息并补发。
            // 注意：不设置也不会影响消息正常收发。
            msg.setKey("ORDERID_" + i);

            // 由于在oneway方式发送消息时没有请求应答处理，如果出现消息发送失败，则会因为没有重试而导致数据丢失。若数据不可丢，建议选用可靠同步或可靠异步发送方式。
            producer.sendOneway(msg);
        }

        // 在应用退出前，销毁Producer对象。
        // 注意：如果不销毁也没有问题。
        producer.shutdown();
    }
}
