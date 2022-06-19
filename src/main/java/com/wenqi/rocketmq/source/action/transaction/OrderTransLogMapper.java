package com.wenqi.rocketmq.source.action.transaction;

/**
 * @author Wenqi Liang
 * @date 2022/6/19
 */
public interface OrderTransLogMapper {
    Integer count(String orderNo);
}
