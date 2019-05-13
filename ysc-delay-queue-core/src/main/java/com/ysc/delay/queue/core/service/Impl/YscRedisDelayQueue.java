package com.ysc.delay.queue.core.service.Impl;

import cn.hutool.core.bean.BeanUtil;
import com.ysc.delay.queue.core.enums.JobStatusEnum;
import com.ysc.delay.queue.core.service.YscDelayQueue;
import com.ysc.delay.queue.core.util.RedisUtil;
import com.ysc.delay.queue.core.vo.DelayQueueDetailInfoVO;
import com.ysc.delay.queue.core.vo.DelayQueueInfoVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @program: ysc-delay-queue
 * @description:
 * @author: shicong yang
 * @create: 2019-05-05 13:32
 **/
@Service
@Slf4j
public class YscRedisDelayQueue implements YscDelayQueue {

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
     * ready queue name
     */
    private String readyQueueName;


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
     * ready job key 前缀
     */
    static final String READY_JOB_PROFIX = "ysc:DQ:rdj:";

    /**
     * 桶数量
     */
    private int bucket;

    private StringRedisTemplate redisTemplate;

    /**
     * 锁超时时间
     */
    private static final int EXPIRE_TIME = 1;

    /**
     * 等待时间
     */
    private long awaitTime;

    private ThreadLocal<String> lockParams = new ThreadLocal<>();

    private ThreadLocal<String> bucketThreadLocal = new ThreadLocal<>();

    /**
     * job pool key name
     */
    private ThreadLocal<String> jobPoolKeyName = new ThreadLocal<>();

    private final transient ReentrantLock lock = new ReentrantLock();
    private final Condition available = lock.newCondition();

    /**
     * description: 组装延迟队列信息。bucket桶个数需要设置<p>
     * param: [delayQueueInfoVO, bucket] <p>
     * return:  <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    public YscRedisDelayQueue(String queueName, int bucket, StringRedisTemplate redisTemplate) {
        this.queueName = queueName;
        this.bucket = bucket;
        this.redisTemplate = redisTemplate;
    }


    public YscRedisDelayQueue(String queueName, StringRedisTemplate redisTemplate) {
        this.queueName = queueName;
        this.redisTemplate = redisTemplate;
        this.bucket = DEFAULT_BUCKET;
    }


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
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final ReentrantLock reLock = this.lock;
        reLock.lock();
        try {
            //1.将信息放入到jobPool，jobPool选用hash
            DelayQueueDetailInfoVO delayQueueDetailInfoVO = buildDelayQueueDetailInfoVO(delayQueueInfoVO);
            //绝对时间 纳秒
            long delayNano = delayQueueInfoVO.getDelayTime() * 1000000000 + System.nanoTime();
            Map<String, Object> jobMap = BeanUtil.beanToMap(delayQueueDetailInfoVO);
            RedisUtil.hPutAll(redisTemplate, JOB_POOL_PREFIX + delayQueueDetailInfoVO.getTopic() + delayQueueDetailInfoVO.getId(), jobMap);
            //2.放入到bucket（轮询放入）
            String bucketName = BUCKET_NAME_PREFIX + queueName + count(delayQueueDetailInfoVO.getTopic() + delayQueueDetailInfoVO.getId(), bucket);
            return RedisUtil.zAdd(redisTemplate, bucketName, delayQueueDetailInfoVO.getTopic() + delayQueueDetailInfoVO.getId(), delayNano);
        } finally {
            available.signal();
            reLock.unlock();
            stopWatch.stop();
            log.info("Spend time {}ms", stopWatch.getTotalTimeMillis());

        }
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
        if (StringUtils.isNotBlank(delayQueueInfoVO.getCallbackUrl())) {
            delayQueueDetailInfoVO.setCallbackUrl(delayQueueInfoVO.getCallbackUrl());
        }
        return delayQueueDetailInfoVO;
    }

    @Override
    public DelayQueueDetailInfoVO pop() throws Exception {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        /**
         * 1.0遍历bucket
         */
        DelayQueueDetailInfoVO delayQueueDetailInfoVO = null;
        //ready job key
        readyQueueName = READY_JOB_PROFIX + queueName;
        final ReentrantLock reLock = this.lock;
        reLock.lock();
        try {
            for (; ; ) {
                if (RedisUtil.zSize(redisTemplate, readyQueueName) > 0) {
                    lockParams.set(queueName + System.currentTimeMillis());
                    try {
                        if (RedisUtil.lock(redisTemplate, LOCK_PREFIX + queueName, lockParams.get(), EXPIRE_TIME)) {
                            Set<String> readyJob = RedisUtil.zRange(redisTemplate, readyQueueName, 0, 0);
                            Iterator<String> readyJobIterator = readyJob.iterator();
                            while (readyJobIterator.hasNext()) {
                                String value = readyJobIterator.next();
                                String key = JOB_POOL_PREFIX + value;
//                                //从redis bucket中取出对象
                                /**
                                 * 返回（从ready job queue 中获取）
                                 */
                                Map<Object, Object> hashValue = RedisUtil.hGetAll(redisTemplate, key);
                                delayQueueDetailInfoVO = BeanUtil.mapToBeanIgnoreCase(hashValue, DelayQueueDetailInfoVO.class, true);
                                //TODO ack 去除hash中值
                                jobPoolKeyName.set(key);

                                if (true) {      //TODO 相关状态判断
                                    //移除ready job 第一个
                                    RedisUtil.zRemoveRange(redisTemplate, readyQueueName, 0, 0);
                                }
                                return delayQueueDetailInfoVO;
                            }
                        }
                    } finally {
                        if (RedisUtil.hasKey(redisTemplate, LOCK_PREFIX + queueName)) {
                            RedisUtil.unLock(redisTemplate, LOCK_PREFIX + queueName, lockParams.get());
                        }
                    }
                } else {
                    for (int i = 0; i < bucket; i++) {
                        String bucketName = BUCKET_NAME_PREFIX + queueName + i;
                        bucketThreadLocal.set(LOCK_PREFIX + bucketName);
                        lockParams.set(bucketName + System.currentTimeMillis());
                        try {
                            Long bucketElemSize = RedisUtil.zSize(redisTemplate, bucketName);
                            if (RedisUtil.lock(redisTemplate, bucketThreadLocal.get(), lockParams.get(), EXPIRE_TIME)) {
                                //第一个
                                Set<ZSetOperations.TypedTuple<String>> typedTuples = RedisUtil.zRangeWithScores(redisTemplate, bucketName, bucketElemSize - 1, bucketElemSize);
                                Iterator<ZSetOperations.TypedTuple<String>> iterator = typedTuples.iterator();
                                while (iterator.hasNext()) {
                                    ZSetOperations.TypedTuple<String> next = iterator.next();
                                    if (next.getScore() <= System.nanoTime()) {
                                        /**
                                         * 小于等于当前时间则到了执行对时候，放入到ready job 队列(zset)中，防止后面丢失以及重新部署后不会执行
                                         */
                                        //TODO 状态判断
                                        Boolean add = RedisUtil.zAdd(redisTemplate, readyQueueName, next.getValue(), next.getScore());
                                        /**
                                         * 从bucket 移除
                                         * TODO ack判断操作
                                         */
                                        if (add) {
                                            RedisUtil.zRemoveRange(redisTemplate, bucketName, bucketElemSize - 1, bucketElemSize);
                                        }
                                    } else {
                                        awaitTime = next.getScore().longValue() - System.nanoTime();
                                        available.awaitNanos(awaitTime);
//                                        Thread.sleep(awaitTime);
                                    }
                                }
                            }
                        } finally {
                            if (StringUtils.isNotBlank(bucketThreadLocal.get())) {
                                if (RedisUtil.hasKey(redisTemplate, bucketThreadLocal.get())) {
                                    RedisUtil.unLock(redisTemplate, bucketThreadLocal.get(), lockParams.get());
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("ysc-delay-queue exception", e);
        } finally {
            available.signal();
            reLock.unlock();
            stopWatch.stop();
            log.info("Spend time {}ms", stopWatch.getTotalTimeMillis());
        }
        return delayQueueDetailInfoVO;
    }


    @Override
    public boolean ack() throws Exception {
        RedisUtil.delete(redisTemplate, jobPoolKeyName.get());
        return true;
    }

    @Override
    public long length() throws Exception {
        /**
         * bucket + readyQueue+tryQueue
         */
        long size = 0L;
        for (int i = 0; i < bucket; i++) {
            String bucketName = BUCKET_NAME_PREFIX + queueName + i;
            size = size + RedisUtil.zSize(redisTemplate, bucketName);
        }
        size = size + RedisUtil.zSize(redisTemplate, readyQueueName);
        //TODO + tryQueue
        return size;
    }

    @Override
    public boolean clean() throws Exception {

        /**
         * 防止死锁，只修改失效时间
         */
//        // del ready queue
//        RedisUtil.zRemoveRange(redisTemplate, readyQueueName, 0, -1);
//        //del bucket queue
//        for (int i = 0; i < bucket; i++) {
//            String bucketName = BUCKET_NAME_PREFIX + queueName + i;
//            RedisUtil.zRemoveRange(redisTemplate, bucketName, 0, -1);
//        }
        RedisUtil.expire(redisTemplate, readyQueueName, 0, TimeUnit.NANOSECONDS);
        for (int i = 0; i < bucket; i++) {
            String bucketName = BUCKET_NAME_PREFIX + queueName + i;
            RedisUtil.expire(redisTemplate, bucketName, 0, TimeUnit.NANOSECONDS);
        }
        return true;
    }

//    @Override
//    public long getDelay() {
//        return 0;
//    }
//
//    @Override
//    public void setDelay(long delay) {
//
//    }
}
