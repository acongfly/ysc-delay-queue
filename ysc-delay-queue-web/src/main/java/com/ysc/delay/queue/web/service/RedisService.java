package com.ysc.delay.queue.web.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ysc.delay.queue.core.service.Impl.YscRedisDelayQueue;
import com.ysc.delay.queue.core.vo.DelayQueueDetailInfoVO;
import com.ysc.delay.queue.core.vo.DelayQueueInfoVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.*;

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

    YscRedisDelayQueue yscRedisDelayQueue;
    private ExecutorService addThreadPool;

    private ExecutorService readThreadPool;

    private static final int POOL_SIZE = 4;// 单个CPU线程池大小
    private static final int MAX_POOL_SIZE = 8;//每个CPU最大线程数

    @PostConstruct
    public void init() {
        yscRedisDelayQueue = new YscRedisDelayQueue("testQueue", stringRedisTemplate);
        ThreadFactory saveTreadFactory = new ThreadFactoryBuilder().setNameFormat("payment-submit-channel-%d").build();
        addThreadPool = new ThreadPoolExecutor(1, 1,
                0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), saveTreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        readThreadPool = new ThreadPoolExecutor(1, 1,
                0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), saveTreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void addRedisHash() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < 10; i++) {
            final int task = i;
            addThreadPool.execute(() -> {
                addParam(task);
                log.info("执行第{}", task);
            });
        }
        stopWatch.stop();
        log.info("执行总耗时{}", stopWatch.getTotalTimeSeconds());


//        List<Thread> threads = new ArrayList<>();
//        //十个线程
//        for (int i = 0; i < 10; i++) {
//            final int ii = i;
//            Thread t = new Thread(() -> {
//                try {
//
//                    long start = System.currentTimeMillis();
//                    for (int j = 0; j < 100; j++) {
//                        DelayQueueDetailInfoVO pop = yscRedisDelayQueue.pop();
//                        log.info("thread name {},{},pop={}", Thread.currentThread(), ii, pop);
//                        yscRedisDelayQueue.ack();
//
//                    }
//                    long timeUsed = System.currentTimeMillis() - start;
//                    log.info("10 threads consume 10000, use {} ms", timeUsed);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//            });
//            threads.add(t);
//        }
//
//        for (Thread t : threads) {
//            t.start();
//        }

//        try {
//            Thread.sleep(600000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

    }

    public void read() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        readThreadPool.execute(() -> {
            while (true) {
                DelayQueueDetailInfoVO pop = null;
                try {
                    pop = yscRedisDelayQueue.pop();
                    log.info("thread name {},pop={}", Thread.currentThread().getName(), pop);
                    yscRedisDelayQueue.ack();
                } catch (Exception e) {
                    log.error("exception info", e);
                }
            }
        });
        stopWatch.stop();
        log.info("执行总耗时{}", stopWatch.getTotalTimeSeconds());
    }

    /**
     * 增加
     */
    private void addParam(int task) {
        try {
            for (int i = 0; i < 100; i++) {
                DelayQueueInfoVO delayQueueInfoVO = new DelayQueueInfoVO();
                delayQueueInfoVO.setTopic("test" + task);
                delayQueueInfoVO.setId(i + "_task_" + task);
                delayQueueInfoVO.setDelayTime(6L);
                delayQueueInfoVO.setTimeToRun(6L);
                delayQueueInfoVO.setBody("");
                delayQueueInfoVO.setType(1 + "");
                yscRedisDelayQueue.push(delayQueueInfoVO);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
