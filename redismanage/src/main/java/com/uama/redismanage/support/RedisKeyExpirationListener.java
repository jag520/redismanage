package com.uama.redismanage.support;

import java.nio.charset.StandardCharsets;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.uama.redismanage.config.RedisConfiguration;

/**
 * @Description:
 * @Author: vancent
 * @Date: 2020/12/10 20:06
 */
@Component
public class RedisKeyExpirationListener extends KeyExpirationEventMessageListener {

    // 配置监听哪个频道
    private static final Topic KEYEVENT_EXPIRED_TOPIC = new PatternTopic("__keyevent@*__:expired");
    private static final Topic KEYEVENT_DEL_TOPIC = new PatternTopic("__keyevent@*__:del");

    public RedisKeyExpirationListener(RedisMessageListenerContainer listenerContainer) {
        super(listenerContainer);
    }

    @Override
    protected void doRegister(RedisMessageListenerContainer listenerContainer) {
        // 频道可以是多，多个传list
        listenerContainer.addMessageListener(this, KEYEVENT_EXPIRED_TOPIC);
        listenerContainer.addMessageListener(this, KEYEVENT_DEL_TOPIC);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        // 事件 __keyevent@10__:del
        String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
        //过期的key
        String key = new String(message.getBody(),StandardCharsets.UTF_8);
        System.out.println(channel + " - " + key);

        // 解析源数据, 对目标库进行操作
        if (!StringUtils.isEmpty(channel) && !StringUtils.isEmpty(key)) {
            String dbIndexStr = channel.replace("__keyevent@", "").replace("__:expired", "").replace("__:del", "");
            Integer dbIndex = Integer.valueOf(dbIndexStr);

            RedisTemplate redisTemplate = RedisConfiguration.getRedisTemplateDestByIndexDb(dbIndex);
//            ValueOperations valueOperations = redisTemplate.opsForValue();
//            valueOperations.set(key, "我是自己新增的" + (dbIndex+1));

            redisTemplate.delete(key);
        }

    }

}
