package com.wenqi.rocketmq.aliyun.consumer;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.PropertyValueConst;

import java.util.Properties;


/**
 * MQ推送到consumer
 *
 * @author liangwenqi
 * @date 2022/3/24
 */
public class PushConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 您在控制台创建的Group ID。
        properties.put(PropertyKeyConst.GROUP_ID, "XXX");
        // AccessKey ID阿里云身份验证，在阿里云RAM控制台创建。
        properties.put(PropertyKeyConst.AccessKey, "XXX");
        // Accesskey Secret阿里云身份验证，在阿里云服RAM控制台创建。
        properties.put(PropertyKeyConst.SecretKey, "XXX");
        // 设置TCP接入域名，进入控制台的实例详情页面的TCP协议客户端接入点区域查看。
        properties.put(PropertyKeyConst.NAMESRV_ADDR, "XXX");

        // 集群订阅方式(默认)。
        // properties.put(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        // 广播订阅方式。
         properties.put(PropertyKeyConst.MessageModel, PropertyValueConst.BROADCASTING);

        Consumer consumer = ONSFactory.createConsumer(properties);
        // 订阅多个Tag。
        consumer.subscribe("TopicTestMQ", "TagA || TagB", new MessageListener() {
            @Override
            public Action consume(Message message, ConsumeContext context) {
                System.out.println("Receive: " + message);
                return Action.CommitMessage;
            }
        });

        // 订阅另外一个Topic，如需取消订阅该Topic，请删除该部分的订阅代码，重新启动消费端即可。
        // 订阅全部Tag。
        consumer.subscribe("TopicTestMQ-Other", "*", new MessageListener() {
            @Override
            public Action consume(Message message, ConsumeContext context) {
                System.out.println("Receive: " + message);
                return Action.CommitMessage;
            }
        });

        consumer.start();
        System.out.println("Consumer Started");
    }
}
