package com.melp.kafka.elasticsearch.transformer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

@Component
public class ESTransformerProp {
	public static final String KAFKA = "kafka.";
	public static final String BUSINESS_TOPIC_STR = "kafka.business.topic";
	public static final String BUSINESS_HOUR_TOPIC_STR = "kafka.business.hour.topic";
	public static final String TARGET_TOPIC_STR = "kafka.target.topic";
	private final Environment env;

	public ESTransformerProp(final Environment env) {
		this.env = env;
	}

	public String getAppId() {
		return env.getProperty(KAFKA + StreamsConfig.APPLICATION_ID_CONFIG);
	}

	public String getBoostrapServer() {
		return env.getProperty(KAFKA + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
	}

	public String getAutoOffsetReset() {
		return env.getProperty(KAFKA + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
	}
	
	public String getDefaultKeySerde() {
		return env.getProperty(
				KAFKA + StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG);
	}
	
	public String getDefaultvalueSerde() {
		return env.getProperty(
				KAFKA + StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG);
	}
	
	public String getSchemaRegistryUrl() {
		return env.getProperty(KAFKA + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
	}
	
	public String getBusinessTopic() {
		return env.getProperty(BUSINESS_TOPIC_STR);
	}
	
	public String getBusinessHourTopic() {
		return env.getProperty(BUSINESS_HOUR_TOPIC_STR);
	}
	
	public String getTargetTopic() {
		return env.getProperty(TARGET_TOPIC_STR);
	}

	public String getProp(final String key) {
		return env.getProperty(key);
	}
}
