package com.wenqi.rocketmq.source.action.transaction;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Wenqi Liang
 * @date 2022/6/19
 */
@Service
public class OrderServiceImpl {
    private TransactionMQProducer mqProducer;

    public OrderServiceImpl() {
        mqProducer = new TransactionMQProducer("order_producer_grpup");
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("order-producer-grpup_msg-check-thread");
                return thread;
            }
        });
        mqProducer.setExecutorService(executorService);
        // 设置事务消息回调监听器
        mqProducer.setTransactionListener((TransactionListener) SpringContextUtils.getBean("orderMessageListener"));
        try {
            mqProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public Map saveOrCreateOrder(Order order) {
        Map result = new HashMap();
        if (order.getBuyerId() == null || order.getBuyerId() < 1) {
            result.put("code", "1");
            // 用户购买者不能为空
            result.put("msg", "用户购买者不能为空");
            return result;
        }
        // 省略其他业务类校验
        try {
            mqProducer.send(new Message("topic_order", JSON.toJSONString(order).getBytes()));
        } catch (Throwable e) {
            e.printStackTrace();
            //可以进行一些重试，在这里直接返回错误
            result.put("code", "1");
            result.put("msg", "系统异常");
            return result;
        }
        result.put("code", 0);
        return result;
    }
}
