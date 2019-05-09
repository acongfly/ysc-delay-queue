package com.ysc.delay.queue.web.service;

import com.ysc.delay.queue.core.service.Impl.YscRedisDelayQueue;
import com.ysc.delay.queue.core.vo.DelayQueueDetailInfoVO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @program: ysc-delay-queue
 * @description:
 * @author: shicong yang
 * @create: 2019-05-05 14:30
 **/
@Service
public class RedisService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void addRedisHash() {

//        HashMap<String, String> map = new HashMap<>();
//        map.put("id", "123");
//        map.put("topic", "test");
//        RedisStaticUtil redisStaticUtil = new RedisStaticUtil(stringRedisTemplate);
        YscRedisDelayQueue yscRedisDelayQueue = new YscRedisDelayQueue("testQueue", stringRedisTemplate);
//        try {
//            for (int i = 0; i < 100; i++) {
//                DelayQueueInfoVO delayQueueInfoVO = new DelayQueueInfoVO();
//                delayQueueInfoVO.setTopic("test");
//                delayQueueInfoVO.setId("12345678" + i);
//                delayQueueInfoVO.setDelayTime(6L);
//                delayQueueInfoVO.setTimeToRun(6L);
//                delayQueueInfoVO.setBody("");
//                delayQueueInfoVO.setType(1 + "");
//                yscRedisDelayQueue.push(delayQueueInfoVO);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

//        new Thread(()->{
        try {
//            while (true){
            for (int i = 0; i < 100; i++) {
                DelayQueueDetailInfoVO pop = yscRedisDelayQueue.pop();
                System.out.println(pop);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        }
//        ).start();


    }
}
