package com.ysc.delay.queue.core.enums;

/**
 * description: job状态<p>
 * param:  <p>
 * return:  <p>
 * author: shicong yang <p>
 * date: 2019-05-04 <p>
 */
public enum JobTypeEnum {

    NOTIFY(1, "通知服务"),
    SILENT(2, "静默"),

    ;

    private final int value;
    private final String description;

    JobTypeEnum(int value, String description) {
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
    public static JobTypeEnum parseOf(int value) {
        for (JobTypeEnum item : values()) {
            if (item.getValue() == value) {
                return item;
            }
        }
        return null;
    }

}
