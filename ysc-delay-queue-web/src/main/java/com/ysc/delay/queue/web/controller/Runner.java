package com.ysc.delay.queue.web.controller;

import com.ysc.delay.queue.web.service.RedisService;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @program: ysc-delay-queue
 * @description: 测试启动多线程处理操作
 * @author: shicong yang
 * @create: 2019-05-16 13:22
 **/
@Component
public class Runner implements ApplicationRunner {

    @Resource
    private RedisService redisService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        redisService.addRedisHash();
//        redisService.read();
    }
}
