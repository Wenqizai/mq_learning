package com.wenqi.rocketmq.aliyun.retry;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;

/**
 * 获取消息重试次数
 *
 * @author liangwenqi
 * @date 2022/3/25
 */
public class MessageListenerImpl3 implements MessageListener {
    @Override
    public Action consume(Message message, ConsumeContext context) {
        // 消息重试次数
        System.out.println(message.getReconsumeTimes());
        return Action.CommitMessage;
    }
}
