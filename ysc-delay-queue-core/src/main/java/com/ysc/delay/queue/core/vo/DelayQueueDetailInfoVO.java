package com.ysc.delay.queue.core.vo;

import lombok.Builder;
import lombok.Data;

/**
 * @program: ysc-delay-queue
 * @description: 延迟队列相关详情
 * @author: shicong yang
 * @create: 2019-05-05 10:28
 **/
@Data
public class DelayQueueDetailInfoVO {
    private static final long serialVersionUID = 7720457514956595013L;
    /**
     * Job类型。可以理解成具体的业务名称
     */
    private String topic;

    /**
     * Job的唯一标识。用来检索和删除指定的Job信息
     */
    private String id;

    /**
     * Job需要延迟的时间。单位：秒。（服务端会将其转换为绝对时间
     */
    private String delayTime;

    /**
     * Job执行超时时间 单位秒.执行失败后进行重试间隔的时间
     */
    private String timeToRun;

    /**
     * Job的内容，供消费者做具体的业务处理，以json格式存储。
     */
    private String body;

    /**
     * 延迟任务类型
     */
    private String type;
    /**
     * 尝试次数
     */
    private String retryTime;

    /**
     * 延迟队列执行状态
     */
    private String status;
}
