package com.melp.checkin;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

@Component
public class CheckinProps {
	public static final String KAFKA = "kafka.";
	public static final String TARGET_TOPIC_STR= "kafka.checkin.topic";

	private final Environment env;

	public CheckinProps(final Environment env) {
		this.env = env;
	}
	
	public String getBoostrapServer() {
		return env.getProperty(KAFKA + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
	}
	
	public String getSchemaRegistryUrl() {
		return env.getProperty(KAFKA + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
	}
	
	public String getTargetTopic() {
		return env.getProperty(TARGET_TOPIC_STR);
	}

	public String getProp(final String key) {
		return env.getProperty(key);
	}
}
