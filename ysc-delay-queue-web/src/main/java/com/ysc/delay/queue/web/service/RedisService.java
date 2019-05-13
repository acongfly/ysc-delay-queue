package com.ysc.delay.queue.web.service;

import com.ysc.delay.queue.core.service.Impl.YscRedisDelayQueue;
import com.ysc.delay.queue.core.vo.DelayQueueDetailInfoVO;
import com.ysc.delay.queue.core.vo.DelayQueueInfoVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: ysc-delay-queue
 * @description:
 * @author: shicong yang
 * @create: 2019-05-05 14:30
 **/
@Service
@Slf4j
public class RedisService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void addRedisHash() {

//        HashMap<String, String> map = new HashMap<>();
//        map.put("id", "123");
//        map.put("topic", "test");
//        RedisStaticUtil redisStaticUtil = new RedisStaticUtil(stringRedisTemplate);
        YscRedisDelayQueue yscRedisDelayQueue = new YscRedisDelayQueue("testQueue", stringRedisTemplate);


//        new Thread(()->{
//        try {
////            while (true){
//            for (int i = 0; i < 100; i++) {
//                DelayQueueDetailInfoVO pop = yscRedisDelayQueue.pop();
//                System.out.println(pop);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        }
//        ).start();


//        try {
//            for (int i = 0; i < 100; i++) {
//                DelayQueueInfoVO delayQueueInfoVO = new DelayQueueInfoVO();
//                delayQueueInfoVO.setTopic("test");
//                delayQueueInfoVO.setId(i+"");
//                delayQueueInfoVO.setDelayTime(6L);
//                delayQueueInfoVO.setTimeToRun(6L);
//                delayQueueInfoVO.setBody("");
//                delayQueueInfoVO.setType(1 + "");
//                yscRedisDelayQueue.push(delayQueueInfoVO);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        List<Thread> threads = new ArrayList<>();
        //十个线程
        for (int i = 0; i < 10; i++) {
            final int ii = i;
            Thread t = new Thread(() -> {
                try {

                    long start = System.currentTimeMillis();
                    for (int j = 0; j < 100; j++) {
                        DelayQueueDetailInfoVO pop = yscRedisDelayQueue.pop();
                        log.info("thread name {},{},pop={}", Thread.currentThread(), ii, pop);
                        yscRedisDelayQueue.ack();

                    }
                    long timeUsed = System.currentTimeMillis() - start;
                    log.info("10 threads consume 10000, use {} ms", timeUsed);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
            threads.add(t);
        }

        for (Thread t : threads) {
            t.start();
        }

        try {
            Thread.sleep(600000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
