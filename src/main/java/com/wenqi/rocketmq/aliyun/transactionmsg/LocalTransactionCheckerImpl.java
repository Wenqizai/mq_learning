package com.wenqi.rocketmq.aliyun.transactionmsg;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;
import lombok.extern.slf4j.Slf4j;

/**
 * @author liangwenqi
 * @date 2022/3/24
 */
@Slf4j
public class LocalTransactionCheckerImpl implements LocalTransactionChecker {
    final  BusinessService businessService = new BusinessService();

    @Override
    public TransactionStatus check(Message msg) {
        //消息ID（有可能消息体一样，但消息ID不一样，当前消息属于半事务消息，所以消息ID在消息队列RocketMQ版控制台无法查询）。
        String msgId = msg.getMsgID();
        //消息体内容进行crc32，也可以使用其它的方法如MD5。
        long crc32Id = HashUtil.crc32Code(msg.getBody());
        //消息ID和crc32Id主要是用来防止消息重复。
        //如果业务本身是幂等的，可以忽略，否则需要利用msgId或crc32Id来做幂等。
        //如果要求消息绝对不重复，推荐做法是对消息体使用crc32或MD5来防止重复消息。
        //业务自己的参数对象，这里只是一个示例，需要您根据实际情况来处理。
        Object businessServiceArgs = new Object();
        TransactionStatus transactionStatus = TransactionStatus.Unknow;
        try {
            boolean isCommit = businessService.checkBusinessService(businessServiceArgs);
            if (isCommit) {
                //本地事务已成功则提交消息。
                transactionStatus = TransactionStatus.CommitTransaction;
            } else {
                //本地事务已失败则回滚消息。
                transactionStatus = TransactionStatus.RollbackTransaction;
            }
        } catch (Exception e) {
            log.error("Message Id:{}", msgId, e);
        }
        log.warn("Message Id:{}transactionStatus:{}", msgId, transactionStatus.name());
        return transactionStatus;
    }
}
