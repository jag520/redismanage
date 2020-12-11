package com.uama.redismanage.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.uama.redismanage.support.RedisConfigProperties;


@Import({RedisConfiguration.SourceRedis.class, RedisConfiguration.DestRedis.class})
@Configuration
public class RedisConfiguration {

	private static Map<Integer, RedisTemplate> redisTemplateMap = new HashMap<>();

	@Autowired
	SourceRedis sourceRedis;
	@Autowired
	DestRedis destRedis;

	@ConfigurationProperties(prefix = "spring.redis.source")
	static class SourceRedis extends RedisConfigProperties {
	}

	@ConfigurationProperties(prefix = "spring.redis.dest")
	static class DestRedis extends RedisConfigProperties {
	}

	@Primary
	@Bean(name = "sourceRedisConnectionFactory")
	public RedisConnectionFactory sourceRedisConnectionFactory(){
		return createRedisConnectionFactory(sourceRedis, 0);
	}

	@Bean("destRedisTemplate")
	public RedisTemplate destRedisTemplate(@Qualifier("destRedisConnectionFactory0") RedisConnectionFactory destRedisConnectionFactory0,
										   @Qualifier("destRedisConnectionFactory1") RedisConnectionFactory destRedisConnectionFactory1,
										   @Qualifier("destRedisConnectionFactory2") RedisConnectionFactory destRedisConnectionFactory2,
										   @Qualifier("destRedisConnectionFactory3") RedisConnectionFactory destRedisConnectionFactory3,
										   @Qualifier("destRedisConnectionFactory4") RedisConnectionFactory destRedisConnectionFactory4,
										   @Qualifier("destRedisConnectionFactory5") RedisConnectionFactory destRedisConnectionFactory5,
										   @Qualifier("destRedisConnectionFactory6") RedisConnectionFactory destRedisConnectionFactory6,
										   @Qualifier("destRedisConnectionFactory7") RedisConnectionFactory destRedisConnectionFactory7,
										   @Qualifier("destRedisConnectionFactory8") RedisConnectionFactory destRedisConnectionFactory8,
										   @Qualifier("destRedisConnectionFactory9") RedisConnectionFactory destRedisConnectionFactory9,
										   @Qualifier("destRedisConnectionFactory10") RedisConnectionFactory destRedisConnectionFactory10,
										   @Qualifier("destRedisConnectionFactory11") RedisConnectionFactory destRedisConnectionFactory11,
										   @Qualifier("destRedisConnectionFactory12") RedisConnectionFactory destRedisConnectionFactory12,
										   @Qualifier("destRedisConnectionFactory13") RedisConnectionFactory destRedisConnectionFactory13,
										   @Qualifier("destRedisConnectionFactory14") RedisConnectionFactory destRedisConnectionFactory14,
										   @Qualifier("destRedisConnectionFactory15") RedisConnectionFactory destRedisConnectionFactory15){

		setRedisTemplateDestByIndexDb(0, destRedisConnectionFactory0);
		setRedisTemplateDestByIndexDb(1, destRedisConnectionFactory1);
		setRedisTemplateDestByIndexDb(2, destRedisConnectionFactory2);
		setRedisTemplateDestByIndexDb(3, destRedisConnectionFactory3);
		setRedisTemplateDestByIndexDb(4, destRedisConnectionFactory4);
		setRedisTemplateDestByIndexDb(5, destRedisConnectionFactory5);
		setRedisTemplateDestByIndexDb(6, destRedisConnectionFactory6);
		setRedisTemplateDestByIndexDb(7, destRedisConnectionFactory7);
		setRedisTemplateDestByIndexDb(8, destRedisConnectionFactory8);
		setRedisTemplateDestByIndexDb(9, destRedisConnectionFactory9);
		setRedisTemplateDestByIndexDb(10, destRedisConnectionFactory10);
		setRedisTemplateDestByIndexDb(11, destRedisConnectionFactory11);
		setRedisTemplateDestByIndexDb(12, destRedisConnectionFactory12);
		setRedisTemplateDestByIndexDb(13, destRedisConnectionFactory13);
		setRedisTemplateDestByIndexDb(14, destRedisConnectionFactory14);
		setRedisTemplateDestByIndexDb(15, destRedisConnectionFactory15);
		return null;
	}

	@Bean(name = "destRedisConnectionFactory0")
	public RedisConnectionFactory destRedisConnectionFactory0(){
		return createRedisConnectionFactory(destRedis, 0);
	}
	@Bean(name = "destRedisConnectionFactory1")
	public RedisConnectionFactory destRedisConnectionFactory1(){
		return createRedisConnectionFactory(destRedis, 1);
	}
	@Bean(name = "destRedisConnectionFactory2")
	public RedisConnectionFactory destRedisConnectionFactory2(){
		return createRedisConnectionFactory(destRedis, 2);
	}
	@Bean(name = "destRedisConnectionFactory3")
	public RedisConnectionFactory destRedisConnectionFactory3(){
		return createRedisConnectionFactory(destRedis, 3);
	}
	@Bean(name = "destRedisConnectionFactory4")
	public RedisConnectionFactory destRedisConnectionFactory4(){
		return createRedisConnectionFactory(destRedis, 4);
	}
	@Bean(name = "destRedisConnectionFactory5")
	public RedisConnectionFactory destRedisConnectionFactory5(){
		return createRedisConnectionFactory(destRedis, 5);
	}
	@Bean(name = "destRedisConnectionFactory6")
	public RedisConnectionFactory destRedisConnectionFactory6(){
		return createRedisConnectionFactory(destRedis, 6);
	}
	@Bean(name = "destRedisConnectionFactory7")
	public RedisConnectionFactory destRedisConnectionFactory7(){
		return createRedisConnectionFactory(destRedis, 7);
	}
	@Bean(name = "destRedisConnectionFactory8")
	public RedisConnectionFactory destRedisConnectionFactory8(){
		return createRedisConnectionFactory(destRedis, 8);
	}
	@Bean(name = "destRedisConnectionFactory9")
	public RedisConnectionFactory destRedisConnectionFactory9(){
		return createRedisConnectionFactory(destRedis, 9);
	}
	@Bean(name = "destRedisConnectionFactory10")
	public RedisConnectionFactory destRedisConnectionFactory10(){
		return createRedisConnectionFactory(destRedis, 10);
	}
	@Bean(name = "destRedisConnectionFactory11")
	public RedisConnectionFactory destRedisConnectionFactory11(){
		return createRedisConnectionFactory(destRedis, 11);
	}
	@Bean(name = "destRedisConnectionFactory12")
	public RedisConnectionFactory destRedisConnectionFactory12(){
		return createRedisConnectionFactory(destRedis, 12);
	}
	@Bean(name = "destRedisConnectionFactory13")
	public RedisConnectionFactory destRedisConnectionFactory13(){
		return createRedisConnectionFactory(destRedis, 13);
	}
	@Bean(name = "destRedisConnectionFactory14")
	public RedisConnectionFactory destRedisConnectionFactory14(){
		return createRedisConnectionFactory(destRedis, 14);
	}
	@Bean(name = "destRedisConnectionFactory15")
	public RedisConnectionFactory destRedisConnectionFactory15(){
		return createRedisConnectionFactory(destRedis, 15);
	}

	@Bean
	RedisMessageListenerContainer container(@Qualifier("sourceRedisConnectionFactory") RedisConnectionFactory sourceRedisConnectionFactory) {
		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(sourceRedisConnectionFactory);
		sourceRedisConnectionFactory.getConnection().serverCommands().setConfig("notify-keyspace-events", "AKE");
		Properties config = sourceRedisConnectionFactory.getConnection().serverCommands().getConfig("notify-keyspace-events");
//		System.out.println("notify-keyspace-events=" + config.getProperty("notify-keyspace-events"));
		return container;
	}

	private RedisConnectionFactory createRedisConnectionFactory(RedisConfigProperties redisConfigProperties, Integer indexDb){
		RedisProperties.Pool pool = new RedisProperties.Pool();
		pool.setMaxActive(redisConfigProperties.getLettuce().getPool().getMaxActive());
		pool.setMaxIdle(redisConfigProperties.getLettuce().getPool().getMaxIdle());
		pool.setMinIdle(redisConfigProperties.getLettuce().getPool().getMinIdle());

		LettuceClientConfiguration.LettuceClientConfigurationBuilder clientConfig = new PoolBuilderFactory().createBuilder(pool);
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
		config.setDatabase(indexDb);

		if (redisConfigProperties.getPassword() != null) {
			config.setPassword(RedisPassword.of(redisConfigProperties.getPassword()));
		}
		config.setPort(redisConfigProperties.getPort());
		config.setHostName(redisConfigProperties.getHost());

		LettuceConnectionFactory factory = new LettuceConnectionFactory(config, clientConfig.build());
		return factory;
	}

	private static class PoolBuilderFactory {
		public LettuceClientConfiguration.LettuceClientConfigurationBuilder createBuilder(
				RedisProperties.Pool properties) {
			return LettucePoolingClientConfiguration.builder().poolConfig(getPoolConfig(properties));
		}

		private GenericObjectPoolConfig getPoolConfig(RedisProperties.Pool properties) {
			GenericObjectPoolConfig config = new GenericObjectPoolConfig();
			config.setMaxTotal(properties.getMaxActive());
			config.setMaxIdle(properties.getMaxIdle());
			config.setMinIdle(properties.getMinIdle());
			if (properties.getMaxWait() != null) {
				config.setMaxWaitMillis(properties.getMaxWait().toMillis());
			}
			return config;
		}
	}

	private void setRedisTemplateDestByIndexDb(Integer dbIndex, RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, String> template = new StringRedisTemplate(connectionFactory);
//		template.setConnectionFactory(connectionFactory);
//		template.setKeySerializer(new StringRedisSerializer());
//		template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
//		template.afterPropertiesSet();
		redisTemplateMap.put(dbIndex, template);
	}

	public static RedisTemplate getRedisTemplateDestByIndexDb(Integer dbIndex){
		return redisTemplateMap.get(dbIndex);
	}
}
