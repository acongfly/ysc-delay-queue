package com.ysc.delay.queue.core.service;

import com.ysc.delay.queue.core.vo.DelayQueueDetailInfoVO;
import com.ysc.delay.queue.core.vo.DelayQueueInfoVO;

/**
 * @program: ysc-delay-queue
 * @description: 延迟队列具体实现
 * @author: shicong yang
 * @create: 2019-05-04 22:24
 **/

public interface YscDelayQueue {

    /**
     * description: 获取队列名称<p>
     * param: [] <p>
     * return: java.lang.String <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    String getQueueName();


    /**
     * description: 放消息job<p>
     * param: [delayQueueInfoVO] <p>
     * return: boolean <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    boolean push(DelayQueueInfoVO delayQueueInfoVO) throws Exception;


    /**
     * description: 从队列头部取出数据<p>
     * param: [] <p>
     * return: com.ysc.delay.queue.core.vo.DelayQueueDetailInfoVO <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    DelayQueueDetailInfoVO pop() throws Exception;

    /**
     * description: 消息执行处理完毕，进行ack<p>
     * param: [] <p>
     * return: boolean <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    boolean ack() throws Exception;


    /**
     * description: 获取队列长度<p>
     * param: [] <p>
     * return: long <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    long length() throws Exception;

    /**
     * description: 清空队列<p>
     * param: [] <p>
     * return: boolean <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    boolean clean() throws Exception;

    /**
     * description: 获取队列延迟时间(s)<p>
     * param: [] <p>
     * return: long <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
//    long getDelay();

    /**
     * description: 重新设置延迟时间<p>
     * param: [delay] <p>
     * return: void <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
//    void setDelay(long delay);


}
