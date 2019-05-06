package com.ysc.delay.queue.core.enums;

/**
 * description: job状态<p>
 * param:  <p>
 * return:  <p>
 * author: shicong yang <p>
 * date: 2019-05-04 <p>
 */
public enum JobStatusEnum {

    READY(1, "可执行状态，等待消费"),
    DELAY(2, "不可执行状态，等待时钟周期"),
    RESERVED(3, "已被消费者读取，但还未得到消费者的响应（delete、finish）"),
    DELETED(4, "已被消费完成或者已被删除"),
    ;

    private final int value;
    private final String description;

    JobStatusEnum(int value, String description) {
        this.value = value;
        this.description = description;

    }

    public int getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    /**
     * 枚举转换
     */
    public static JobStatusEnum parseOf(int value) {
        for (JobStatusEnum item : values()) {
            if (item.getValue() == value) {
                return item;
            }
        }
        return null;
    }

}
