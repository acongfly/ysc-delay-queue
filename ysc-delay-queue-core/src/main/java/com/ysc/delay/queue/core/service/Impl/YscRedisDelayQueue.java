package com.ysc.delay.queue.core.service.Impl;

import cn.hutool.core.bean.BeanUtil;
import com.ysc.delay.queue.core.enums.JobStatusEnum;
import com.ysc.delay.queue.core.service.YscDelayQueue;
import com.ysc.delay.queue.core.util.RedisUtil;
import com.ysc.delay.queue.core.vo.DelayQueueDetailInfoVO;
import com.ysc.delay.queue.core.vo.DelayQueueInfoVO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @program: ysc-delay-queue
 * @description:
 * @author: shicong yang
 * @create: 2019-05-05 13:32
 **/
@Service
public class YscRedisDelayQueue<T extends StringRedisTemplate> implements YscDelayQueue {

    private static ConcurrentHashMap<String, Integer> routePutBucket = new ConcurrentHashMap<String, Integer>();
    private static long CACHE_VALID_TIME = 0;

    /**
     * 默认的bucket个数 8
     */
    static final int DEFAULT_BUCKET = 1 << 3;

    /**
     * 队列名称
     */
    private String queueName;

    /**
     * 桶名称前缀
     */
    static final String BUCKET_NAME_PREFIX = "ysc:DQ:bucket:";

    /**
     * 分布式锁前缀
     */
    static final String LOCK_PREFIX = "ysc:DQ:lock:";

    /**
     * jobPool key 前缀
     */
    static final String JOB_POOL_PREFIX = "ysc:DQ:jp:";

    /**
     * 桶数量
     */
    private int bucket;
    /**
     * 默认bucket的score
     */
    static final int DEFAULT_SCORE = 0;
    /**
     * 默认bucket值
     */
    static final String DEFAULT_BUCKET_VALUE = "default";

    private T redisTemplate;

    /**
     * 锁超时时间
     */
    private static final int EXPIRE_TIME = 60000;

    /**
     * description: 组装延迟队列信息。bucket桶个数需要设置<p>
     * param: [delayQueueInfoVO, bucket] <p>
     * return:  <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    public YscRedisDelayQueue(String queueName, int bucket, T redisTemplate) {
        this.queueName = queueName;
        this.bucket = bucket;
        this.redisTemplate = redisTemplate;
//        createDefaultBucket(queueName, bucket, redisTemplate);
    }


    public YscRedisDelayQueue(String queueName, T redisTemplate) {
        this.queueName = queueName;
        this.redisTemplate = redisTemplate;
        this.bucket = DEFAULT_BUCKET;
//        createDefaultBucket(queueName, DEFAULT_BUCKET, redisTemplate);
    }

//    /**
//     * 创建空的桶
//     *
//     * @param queueName
//     * @param bucket
//     * @param redisTemplate
//     */
//    private void createDefaultBucket(String queueName, int bucket, StringRedisTemplate redisTemplate) {
//        for (int i = 0; i < bucket; i++) {
//            String bucketName = BUCKET_NAME_PREFIX + queueName + i;
//            RedisUtil.zAdd(redisTemplate, bucketName, DEFAULT_BUCKET_VALUE, DEFAULT_SCORE);
//        }
//    }

    @Override
    public String getQueueName() {
        return this.queueName;
    }


    /**
     * 清除缓存，以及轮询添加,获取缓存中存储到哪个bucket
     *
     * @param jobId
     * @param bucket
     * @return
     */
    private static int count(String jobId, int bucket) {
        // cache clear
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            routePutBucket.clear();
            CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
        }
        // count++
        Integer count = routePutBucket.get(jobId);
        // 初始化时主动Random一次，缓解首次压力
        count = (count == null || count > bucket) ? (new SecureRandom().nextInt(bucket)) : ++count;
        routePutBucket.put(jobId, count);
        return count;
    }

    @Override
    public boolean push(DelayQueueInfoVO delayQueueInfoVO) throws Exception {
        String lockParam = queueName + System.currentTimeMillis();
//        try {
//            RedisUtil.lock(redisTemplate, LOCK_PREFIX + queueName, lockParam, EXPIRE_TIME);
        //1.将信息放入到jobPool，jobPool选用hash
        DelayQueueDetailInfoVO delayQueueDetailInfoVO = buildDelayQueueDetailInfoVO(delayQueueInfoVO);
        //绝对时间 纳秒
        long delayNano = delayQueueInfoVO.getDelayTime() * 1000000 + System.nanoTime();
        Map<String, Object> jobMap = BeanUtil.beanToMap(delayQueueDetailInfoVO);
        RedisUtil.hPutAll(redisTemplate, JOB_POOL_PREFIX + delayQueueDetailInfoVO.getTopic() + delayQueueDetailInfoVO.getId(), jobMap);
        //2.放入到bucket（轮询放入）
        String bucketName = BUCKET_NAME_PREFIX + queueName + count(delayQueueDetailInfoVO.getTopic() + delayQueueDetailInfoVO.getId(), bucket);
        return RedisUtil.zAdd(redisTemplate, bucketName, delayQueueDetailInfoVO.getTopic() + delayQueueDetailInfoVO.getId(), delayNano);
//        } finally {
//            RedisUtil.unLock(redisTemplate, LOCK_PREFIX + queueName, lockParam);
//        }
    }

    /**
     * 组装delayQueueDetail
     *
     * @param delayQueueInfoVO
     * @return
     */
    private DelayQueueDetailInfoVO buildDelayQueueDetailInfoVO(DelayQueueInfoVO delayQueueInfoVO) {
        DelayQueueDetailInfoVO delayQueueDetailInfoVO = new DelayQueueDetailInfoVO();
        delayQueueDetailInfoVO.setTopic(delayQueueInfoVO.getTopic());
        delayQueueDetailInfoVO.setId(delayQueueInfoVO.getId());
        delayQueueDetailInfoVO.setBody(delayQueueInfoVO.getBody());
        delayQueueDetailInfoVO.setType(delayQueueInfoVO.getType());
        delayQueueDetailInfoVO.setDelayTime(delayQueueInfoVO.getDelayTime().toString());
        delayQueueDetailInfoVO.setTimeToRun(delayQueueInfoVO.getTimeToRun().toString());
        delayQueueDetailInfoVO.setStatus(JobStatusEnum.READY.getValue() + "");
        delayQueueDetailInfoVO.setRetryTime(0 + "");
        return delayQueueDetailInfoVO;
    }

    @Override
    public DelayQueueDetailInfoVO pop() throws Exception {
        //


        return null;
    }

    @Override
    public boolean ack() throws Exception {
        return false;
    }

    @Override
    public long length() throws Exception {
        return 0;
    }

    @Override
    public boolean clean() throws Exception {
        return false;
    }

    @Override
    public long getDelay() {
        return 0;
    }

    @Override
    public void setDelay(long delay) {

    }
}
