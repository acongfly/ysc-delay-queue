package com.ysc.delay.queue.core.vo;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * @program: ysc-delay-queue
 * @description: 延迟队列vo
 * @author: shicong yang
 * @create: 2019-05-04 21:23
 **/
@Data
public class DelayQueueInfoVO implements Serializable {
    private static final long serialVersionUID = 6736841566533255631L;

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
    private Long delayTime;

    /**
     * Job执行超时时间 单位秒.执行失败后进行重试间隔的时间
     */
    private Long timeToRun;

    /**
     * Job的内容，供消费者做具体的业务处理，以json格式存储。
     */
    private String body;

    /**
     * 延迟任务类型
     */
    private String type;

    /**
     * 回调通知
     */
    private String callbackUrl;

}
