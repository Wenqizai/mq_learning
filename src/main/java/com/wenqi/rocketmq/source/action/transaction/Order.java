package com.wenqi.rocketmq.source.action.transaction;

/**
 * @author Wenqi Liang
 * @date 2022/6/19
 */
public class Order {
    private Integer buyerId;
    private String orderNo;

    public Integer getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(Integer buyerId) {
        this.buyerId = buyerId;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }
}
