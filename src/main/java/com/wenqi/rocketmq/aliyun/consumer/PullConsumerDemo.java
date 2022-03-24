package com.wenqi.rocketmq.aliyun.consumer;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.PullConsumer;
import com.aliyun.openservices.ons.api.TopicPartition;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * consumer从MQ拉取消息
 *
 * @author liangwenqi
 * @date 2022/3/24
 */
public class PullConsumerDemo {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.GROUP_ID, "GID-xxxxx");
        // AccessKey ID阿里云身份验证，在阿里云RAM控制台创建。
        properties.put(PropertyKeyConst.AccessKey, "xxxxxxx");
        // AccessKey Secret阿里云身份验证，在阿里云RAM控制台创建。
        properties.put(PropertyKeyConst.SecretKey, "xxxxxxx");
        // 设置TCP接入域名，进入消息队列RocketMQ版控制台的实例详情页面的TCP协议客户端接入点区域查看。
        properties.put(PropertyKeyConst.NAMESRV_ADDR, "xxxxx");
        PullConsumer consumer = ONSFactory.createPullConsumer(properties);
        // 启动Consumer。
        consumer.start();
        // 获取topic-xxx下的所有分区。
        Set<TopicPartition> topicPartitions = consumer.topicPartitions("topic-xxx");
        // 指定需要拉取消息的分区。
        consumer.assign(topicPartitions);

        while (true) {
            // 拉取消息，超时时间为3000 ms。
            List<Message> messages = consumer.poll(3000);
            System.out.printf("Received message: %s %n", messages);
        }
    }
}
