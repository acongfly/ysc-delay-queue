package com.ysc.delay.queue.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @program: ysc-delay-queue
 * @description:
 * @author: shicong yang
 * @create: 2019-05-04 18:50
 **/
@SpringBootApplication
@EnableRedisRepositories
@EnableScheduling
public class YscDelayQueueWebApplication {
    public static void main(String[] args) {
        SpringApplication.run(YscDelayQueueWebApplication.class, args);
    }

}
