package com.melp.kafka.elasticsearch.transformer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;


@Configuration
public class ESTransformerConfiguration {
	  @Bean
	  ApplicationRunner run(final ESTransformerProp prop) {
		    return args -> {
		        Properties config = new Properties();
		        config.put(StreamsConfig.APPLICATION_ID_CONFIG, prop.getAppId());
		        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getBoostrapServer());
		        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop.getAutoOffsetReset());
		        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, prop.getDefaultKeySerde());
		        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, prop.getDefaultvalueSerde());
		        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, prop.getSchemaRegistryUrl());
				ESTransformerKafkaStream esTransformerStream = new ESTransformerKafkaStream(
						prop.getBusinessTopic(), prop.getBusinessHourTopic(), prop.getTargetTopic(), null,
						prop.getSchemaRegistryUrl());
				esTransformerStream.start(config);
		    };
	  }
}
