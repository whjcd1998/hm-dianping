package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 尝试获得redis的互斥锁
     * @param key 锁对应的键
     * @return true：成功获得锁 false：未获得锁
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 解锁
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 返回null防止缓存穿透
     * @param id
     * @return
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> function,
                                          Long time, TimeUnit timeUnit){
        String key = keyPrefix + id;
        String resultJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(resultJson)){
            return JSONUtil.toBean(resultJson, type);
        }
        if (resultJson != null) {
            return null;
        }
        R result = function.apply(id);
        if (result == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        this.set(key, result, time, timeUnit);
        return result;
    }

    /**
     * 逻辑过期防止缓存击穿
     * @param id
     * @return
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> function,
                                          Long time, TimeUnit timeUnit){
        String key = keyPrefix + id;
        String resultJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(resultJson)){
            return null;
        }
        RedisData redisData = JSONUtil.toBean(resultJson, RedisData.class);
        R result = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        if(expireTime.isAfter(LocalDateTime.now())){
            return result;
        }
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if(isLock){
            resultJson = stringRedisTemplate.opsForValue().get(key);
            redisData = JSONUtil.toBean(resultJson, RedisData.class);
            result = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            expireTime = redisData.getExpireTime();

            if(expireTime.isAfter(LocalDateTime.now())){
                unlock(lockKey);
                return result;
            }
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    // 查询数据库
                    R newR = function.apply(id);
                    // 重建缓存
                    this.setWithLogicalExpire(key, newR, time, timeUnit);
                    //模拟耗时
                    Thread.sleep(500);
                } catch (InterruptedException e) {

                } finally {
                    unlock(lockKey);
                }
            });

        }
        return result;
    }

    /**
     * 互斥锁防止缓存击穿
     * @param id 商铺id
     * @return 商店信息 null：未查询到相关信息
     */
    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> function,
                                    Long time, TimeUnit timeUnit){
        String key = keyPrefix + id;
        String resultJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(resultJson)){
            return JSONUtil.toBean(resultJson, type);
        }
        if (resultJson != null) {
            return null;
        }

        String lockKey = LOCK_SHOP_KEY + id;
        R result = null;
        try {
            boolean isLock = tryLock(lockKey);
            if(!isLock){
                //不断轮询
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, function, time, timeUnit);
            }
            resultJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(resultJson)){
                return JSONUtil.toBean(resultJson, type);
            }
            if (resultJson != null) {
                return null;
            }

            result = function.apply(id);
            if (result == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            this.set(key, result, time, timeUnit);
            //模拟耗时
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(lockKey);
        }
        return result;
    }
}
