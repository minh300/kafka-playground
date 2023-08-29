package com.melp.kafka.elasticsearch.transformer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import static com.melp.kafka.elasticsearch.transformer.ESTransformerProp.*;

public class ESTransformerPropTest {
	private static ESTransformerProp prop;

	@BeforeAll
	public static void setup() {
		Environment env = mock(Environment.class);
		when(env.getProperty(KAFKA + StreamsConfig.APPLICATION_ID_CONFIG))
				.thenReturn("appId");
		when(env.getProperty(KAFKA + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
				.thenReturn("localhost");
		when(env.getProperty(KAFKA + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
				.thenReturn("earliest");
		when(env.getProperty(
				KAFKA + StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG))
				.thenReturn("keySerde");
		when(env.getProperty(
				KAFKA + StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG))
				.thenReturn("valueSerde");
		when(env.getProperty(KAFKA
				+ AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
				.thenReturn("schemaUrl");
		when(env.getProperty(BUSINESS_TOPIC_STR)).thenReturn("business");
		when(env.getProperty(BUSINESS_HOUR_TOPIC_STR))
				.thenReturn("business_hour");
		when(env.getProperty(TARGET_TOPIC_STR)).thenReturn("target");
		prop = new ESTransformerProp(env);
	}

	@Test
	public void getAppIdTest() {
		assertEquals("appId", prop.getAppId());
	}
	@Test
	public void getBoostrapServerTest() {
		assertEquals("localhost", prop.getBoostrapServer());
	}
	@Test
	public void getAutoOffsetResetTest() {
		assertEquals("earliest", prop.getAutoOffsetReset());
	}
	@Test
	public void getDefaultKeySerdeTest() {
		assertEquals("keySerde", prop.getDefaultKeySerde());
	}
	@Test
	public void getDefaultvalueSerdeTest() {
		assertEquals("valueSerde", prop.getDefaultvalueSerde());
	}
	@Test
	public void getSchemaRegistryUrlTest() {
		assertEquals("schemaUrl", prop.getSchemaRegistryUrl());
	}
	@Test
	public void getBusinessTopicTest() {
		assertEquals("business", prop.getBusinessTopic());
	}
	@Test
	public void getBusinessHourTopicTest() {
		assertEquals("business_hour", prop.getBusinessHourTopic());
	}
	@Test
	public void getTargetTopicTest() {
		assertEquals("target", prop.getTargetTopic());
	}
}
