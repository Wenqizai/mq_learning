package com.wenqi.rocketmq.source.action.filter;


import org.apache.rocketmq.common.filter.FilterContext;
import org.apache.rocketmq.common.filter.MessageFilter;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 类过滤模式 : 自定义消息过滤类
 * @author Wenqi Liang
 * @date 2022/6/18
 */
public class MessageFilterImpl implements MessageFilter {

    @Override
    public boolean match(MessageExt msg, FilterContext context) {
        String property = msg.getProperty("SequenceId");
        if (property != null) {
            int id = Integer.parseInt(property);
            if (((id % 10) == 0) &&
                    (id > 100)) {
                return true;
            }
        }
        return false;
    }
}
