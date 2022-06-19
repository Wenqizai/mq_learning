package com.wenqi.rocketmq.source.action.transaction;

import java.util.Date;

/**
 * @author Wenqi Liang
 * @date 2022/6/19
 */
public class OrderTransLog {
    private String unionCode;
    private Date createDate;

    public String getUnionCode() {
        return unionCode;
    }

    public void setUnionCode(String unionCode) {
        this.unionCode = unionCode;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }
}
