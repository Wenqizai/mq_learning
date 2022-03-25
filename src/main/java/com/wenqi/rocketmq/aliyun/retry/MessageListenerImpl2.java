package com.wenqi.rocketmq.aliyun.retry;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;

/**
 * 消费投递失败后无需重试 (return Action.CommitMessage;)
 *
 * @author liangwenqi
 * @date 2022/3/25
 */
public class MessageListenerImpl2 implements MessageListener {
    @Override
    public Action consume(Message message, ConsumeContext context) {
        // 消息消费逻辑
        try {
            doConsumeMessage(message);
        } catch (Throwable e) {
            // 消息消费异常, 无需重试, 返回确认消息
            return Action.CommitMessage;
        }
        // 消息消费正常, 返回确认消息
        return Action.CommitMessage;
    }

    private void doConsumeMessage(Message message) {
    }
}
