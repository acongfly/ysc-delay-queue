package com.ysc.delay.queue.core.util;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @program: ysc-delay-queue
 * @description: redis工具类
 * @author: shicong yang
 * @create: 2019-05-05 14:59
 **/
@Component
public class RedisUtil {

    private static final Long UNLOCK_MSG = 1L;

    /**
     * ===============hash操作======================================================
     */
    /**
     * description: hash push 单个属性<p>
     * param: [redisTemplate, key, hashKey, value] <p>
     * return: void <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    public static <V> void hPut(RedisTemplate<String, V> redisTemplate, String key, V hashKey, String value) {
        redisTemplate.opsForHash().put(key, hashKey, value);
    }

    /**
     * description: hash push对象<p>
     * param: [redisTemplate, key, maps] <p>
     * return: void <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    public static void hPutAll(RedisTemplate<String, String> redisTemplate, String key, Map<String, Object> maps) {
        redisTemplate.opsForHash().putAll(key, maps);
    }


    /**
     * 获取存储在哈希表中指定字段的值
     *
     * @param key
     * @param field
     * @return
     */
    public static Object hGet(RedisTemplate<String, String> redisTemplate, String key, String field) {
        return redisTemplate.opsForHash().get(key, field);
    }

    /**
     * 获取所有给定字段的值
     *
     * @param key
     * @return
     */
    public static Map<Object, Object> hGetAll(RedisTemplate<String, String> redisTemplate, String key) {
        return redisTemplate.opsForHash().entries(key);
    }

    /**
     * =========================下面为set操作=============================================
     */

    /**
     * set添加元素
     *
     * @param key
     * @param values
     * @return
     */
    public static Long sAdd(RedisTemplate<String, String> redisTemplate, String key, String... values) {
        return redisTemplate.opsForSet().add(key, values);
    }

    /**
     * set移除元素
     *
     * @param key
     * @param values
     * @return
     */
    public static Long sRemove(RedisTemplate<String, String> redisTemplate, String key, Object... values) {
        return redisTemplate.opsForSet().remove(key, values);
    }

    /**
     * 获取集合的大小
     *
     * @param key
     * @return
     */
//    public static Long sSize(String key) {
//        return RedisUtil.stringRedisTemplate.opsForSet().size(key);
//    }
    /**
     * ======================下面为zset操作===============================================
     */


    /**
     * description: 使用zset操作，自动排序添加。添加元素,有序集合是按照元素的score值由小到大排列<p>
     * param: [redisTemplate, key, value, score] <p>
     * return: java.lang.Boolean <p>
     * author: shicong yang <p>
     * date: 2019-04-25 <p>
     */
    public static <V> Boolean zAdd(RedisTemplate<String, V> redisTemplate, String key, V value, double score) {
        return redisTemplate.opsForZSet().add(key, value, score);
    }

    /**
     * description: zset获取集合的元素, 从小到大排序<p>
     * param: [redisTemplate, key, start 开始位置, end 结束位置, -1查询所有] <p>
     * return: java.util.Set<V> <p>
     * author: shicong yang <p>
     * date: 2019-04-25 <p>
     */
    public static <V> Set<V> zRange(RedisTemplate<String, V> redisTemplate, String key, long start, long end) {
        return redisTemplate.opsForZSet().range(key, start, end);
    }


    /**
     * description: 获取集合的元素, 从大到小排序<p>
     * param: [redisTemplate, key, start, end] <p>
     * return: java.util.Set<V> <p>
     * author: shicong yang <p>
     * date: 2019-04-25 <p>
     */
    public static <V> Set<V> zReverseRange(RedisTemplate<String, V> redisTemplate, String key, long start, long end) {
        return redisTemplate.opsForZSet().reverseRange(key, start, end);
    }

    /**
     * description: 获取集合大小<p>
     * param: [redisTemplate, key] <p>
     * return: java.lang.Long <p>
     * author: shicong yang <p>
     * date: 2019-04-25 <p>
     */
    public static Long zSize(RedisTemplate<String, String> redisTemplate, String key) {
        return redisTemplate.opsForZSet().size(key);
    }

    /**
     * description: 移除指定索引位置的成员<p>
     * param: [redisTemplate, key, start, end] <p>
     * return: java.lang.Long <p>
     * author: shicong yang <p>
     * date: 2019-04-25 <p>
     */
    public static <V> Long zRemoveRange(RedisTemplate<String, V> redisTemplate, String key, long start, long end) {
        return redisTemplate.opsForZSet().removeRange(key, start, end);
    }

    /**
     * 获取集合元素, 并且把score值也获取
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public static Set<TypedTuple<String>> zRangeWithScores(RedisTemplate<String, String> redisTemplate, String key, long start,
                                                           long end) {
        return redisTemplate.opsForZSet().rangeWithScores(key, start, end);
    }

    /**
     * 根据Score值查询集合元素, 从大到小排序
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public static Set<TypedTuple<String>> zReverseRangeByScoreWithScores(RedisTemplate<String, String> redisTemplate,
                                                                         String key, double min, double max) {
        return redisTemplate.opsForZSet().reverseRangeByScoreWithScores(key,
                min, max);
    }

    /**
     * description: 加锁<p>
     * param: [key, request, expireMinute] <p>
     * return: boolean <p>
     * author: shicong yang <p>
     * date: 2019-05-05 <p>
     */
    public static boolean lock(RedisTemplate<String, String> redisTemplate, String key, String request, long expireMinute) {
        return redisTemplate.opsForValue().setIfAbsent(key, request, expireMinute, TimeUnit.MINUTES);
    }


    /**
     * description: 解锁，使用lua脚本，此会比较redis中的值与传进去的值是否相等<p>
     * param: [key, request] <p>
     * return: boolean <p>
     * author: shicong yang <p>
     * date: 2019-04-29 <p>
     */
    public static boolean unLock(RedisTemplate<String, String> redisTemplate, String key, String value) {
        /**
         * 方法一，此时脚本的返回类型必须是Long，这个是ReturnType决定的，源码如下：
         * import ;
         * public static ReturnType fromJavaType(@Nullable Class<?> javaType) {
         *
         * 		if (javaType == null) {
         * 			return ReturnType.STATUS;
         *                }
         * 		if (javaType.isAssignableFrom(List.class)) {
         * 			return ReturnType.MULTI;
         *        }
         * 		if (javaType.isAssignableFrom(Boolean.class)) {
         * 			return ReturnType.BOOLEAN;
         *        }
         * 		if (javaType.isAssignableFrom(Long.class)) {
         * 			return ReturnType.INTEGER;
         *        }
         * 		return ReturnType.VALUE;* 	}
         */
        String script = ScriptUtil.getScript("lua/lock.lua");
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(script);
        redisScript.setResultType(Long.class);
        Long execute = redisTemplate.execute(redisScript, Collections.singletonList(key), value);
        if (Objects.equals(execute, UNLOCK_MSG)) {
            return true;
        }
        return false;
    }
}
