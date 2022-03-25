package com.wenqi.rocketmq.aliyun.retry;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;

/**
 * 期待重试的处理方式
 * @author liangwenqi
 * @date 2022/3/25
 */
public class MessageListenerImpl1 implements MessageListener {
    @Override
    public Action consume(Message message, ConsumeContext context) {
        //消息处理逻辑抛出异常，消息将重试。(消息消费逻辑)
         doConsumeMessage(message);

        //方式1：返回Action.ReconsumeLater，消息将重试。(推荐使用)
        return Action.ReconsumeLater;

        //方式2：返回null，消息将重试。
        //return null;

        //方式3：直接抛出异常，消息将重试。
        //throw new RuntimeException("Consumer Message exception");
    }

    private void doConsumeMessage(Message message) {
    }
}
