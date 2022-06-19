package com.wenqi.rocketmq.source.action.transaction;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * @author Wenqi Liang
 * @date 2022/6/19
 */
@Component
public class OrderMessageListener implements TransactionListener {

    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private OrderTransLogMapper orderTransLogMapper;

    /**
     * 该方法需要被包含在事务中
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        Order order = JSON.parseObject(new String(msg.getBody()), Order.class);
        // 进行一系列业务处理
        orderMapper.insertOrder(order);
        OrderTransLog log = new OrderTransLog();
        log.setUnionCode(order.getOrderNo());
        log.setCreateDate(new Date(System.currentTimeMillis()));
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Order order = JSON.parseObject(new String(msg.getBody()),
                Order.class);
        if (orderTransLogMapper.count(order.getOrderNo()) > 0) {
            return LocalTransactionState.COMMIT_MESSAGE;
        } else {
            /**
             * 事务消息在回查指定次数后，会自动回滚该消息
             */
            return LocalTransactionState.UNKNOW;
        }
    }
}
