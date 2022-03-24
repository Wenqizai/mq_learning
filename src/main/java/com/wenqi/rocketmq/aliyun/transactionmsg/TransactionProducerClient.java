package com.wenqi.rocketmq.aliyun.transactionmsg;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionExecuter;
import com.aliyun.openservices.ons.api.transaction.TransactionProducer;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author liangwenqi
 * @date 2022/3/24
 */
@Slf4j
public class TransactionProducerClient {
    public static void main(String[] args) throws InterruptedException {
        final BusinessService businessService = new BusinessService(); // 本地业务。
        Properties properties = new Properties();
        // 您在消息队列RocketMQ版控制台创建的Group ID。注意：事务消息的Group ID不能与其他类型消息的Group ID共用。
        properties.put(PropertyKeyConst.GROUP_ID, "XXX");
        // AccessKey ID阿里云身份验证，在阿里云RAM控制台创建。
        properties.put(PropertyKeyConst.AccessKey, "XXX");
        // AccessKey Secret阿里云身份验证，在阿里云RAM控制台创建。
        properties.put(PropertyKeyConst.SecretKey, "XXX");
        // 设置TCP接入域名，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看。
        properties.put(PropertyKeyConst.NAMESRV_ADDR, "XXX");

        TransactionProducer producer = ONSFactory.createTransactionProducer(properties,
                new LocalTransactionCheckerImpl());
        producer.start();
        Message msg = new Message("Topic", "TagA", "Hello MQ transaction===".getBytes());
        try {
            SendResult sendResult = producer.send(msg, new LocalTransactionExecuter() {
                @Override
                public TransactionStatus execute(Message msg, Object arg) {
                    // 消息ID（有可能消息体一样，但消息ID不一样，当前消息属于半事务消息，所以消息ID在消息队列RocketMQ版控制台无法查询）。
                    String msgId = msg.getMsgID();
                    // 消息体内容进行crc32，也可以使用其它的如MD5。
                    long crc32Id = HashUtil.crc32Code(msg.getBody());
                    // 消息ID和crc32id主要是用来防止消息重复。
                    // 如果业务本身是幂等的，可以忽略，否则需要利用msgId或crc32Id来做幂等。
                    // 如果要求消息绝对不重复，推荐做法是对消息体使用crc32或MD5来防止重复消息。
                    Object businessServiceArgs = new Object();
                    TransactionStatus transactionStatus = TransactionStatus.Unknow;
                    try {
                        boolean isCommit =
                                businessService.execBusinessService(businessServiceArgs);
                        if (isCommit) {
                            // 本地事务已成功则提交消息。
                            transactionStatus = TransactionStatus.CommitTransaction;
                        } else {
                            // 本地事务已失败则回滚消息。
                            transactionStatus = TransactionStatus.RollbackTransaction;
                        }
                    } catch (Exception e) {
                        log.error("Message Id:{}", msgId, e);
                    }
                    System.out.println(msg.getMsgID());
                    log.warn("Message Id:{}transactionStatus:{}", msgId, transactionStatus.name());
                    return transactionStatus;
                }
            }, null);
        } catch (Exception e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
            System.out.println(new Date() + " Send mq message failed. Topic is:" + msg.getTopic());
            e.printStackTrace();
        }
        // demo example防止进程退出（实际使用不需要这样）。
        TimeUnit.MILLISECONDS.sleep(Integer.MAX_VALUE);
    }
}
