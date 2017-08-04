package com.qf.executor;

import com.google.common.util.concurrent.RateLimiter;

/**
 * 版权：Copyright 2017 QuarkFinance IT
 * 描述：<描述>
 * 创建人：ShaozeWang
 * 创建时间：2017/8/2
 * 备注:
 */
public interface PriorityTaskExecutorMBean {
    /*
    获取已经处理过的task数量
     */
    long getCount();
    /*
    获取队列积压数量
     */
    int getQueueSize();
    /*
    获取线程池大小
     */
    int getThreadPoolSize();
    /*
    修改线程池大小
     */
    void resize(int newSize);

    /*
    打印当前等待队列，有序
     */
    String printQueue();

    /*
    获取QPS
     */
    double getRateLimit();

    /*
    设置QPS
     */
    void resetRate(double permitsPerSecond);

    /*
    禁用QPS限制
     */
    void disableRateLimiter();
}
