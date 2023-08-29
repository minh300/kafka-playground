package com.melp.kafka.elasticsearch.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.melp.avro.Business;
import com.melp.avro.Business_ES;
import com.melp.avro.Business_hour;
import com.melp.avro.MelpAvroUtils;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.*;

public class ESTransformerKafkaStreamTest {
	private static final String SCHEMA_REGISTRY_SCOPE = ESTransformerKafkaStreamTest.class
			.getName();
	private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://"
			+ SCHEMA_REGISTRY_SCOPE;
	private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
	private ESTransformerKafkaStream estransformerStream;
	TopologyTestDriver testDriver;
	TestInputTopic<String, Business> businessTopic;
	TestInputTopic<String, Business_hour> businessHourTopic;
	SpecificAvroDeserializer avroDeserializer;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@BeforeEach
	public void setup() throws IOException, RestClientException {
		schemaRegistryClient.register("business", new AvroSchema(Business.SCHEMA$));
		schemaRegistryClient.register("business_hour", new AvroSchema(Business_hour.SCHEMA$));
		schemaRegistryClient.register("target", new AvroSchema(Business_ES.SCHEMA$));

		estransformerStream = new ESTransformerKafkaStream("business",
				"business_hour", "target", schemaRegistryClient,
				MOCK_SCHEMA_REGISTRY_URL);

		Properties props = new Properties();
		props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
				"maxAggregation");
		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				SpecificAvroSerde.class.getName());
		props.setProperty(
				AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
				MOCK_SCHEMA_REGISTRY_URL);

		testDriver = new TopologyTestDriver(
				estransformerStream.createTopology(), props);

		// avro serializers
		final Map<String, String> config = Collections
				.singletonMap("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
		SpecificAvroSerializer avroSerial = new SpecificAvroSerializer();
		avroSerial.configure(config, false);

		avroDeserializer = new SpecificAvroDeserializer();
		avroDeserializer.configure(config, false);
		// business topic
		businessTopic = testDriver.createInputTopic("business",
				new StringSerializer(), avroSerial);

		businessHourTopic = testDriver.createInputTopic("business_hour",
				new StringSerializer(), avroSerial);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void streamTest() throws IOException, RestClientException {
		String businessJson = "{\"business_id\":\"_rkjLrft2ZgHcLBLF9amXw\",\"name\":\"Uno Pizzeria & Grill\",\"address\":\"106 Black Rock Road\",\"city\":\"Oaks\",\"state\":\"PA\",\"postal_code\":\"19456\",\"latitude\":40.1345642784,\"longitude\":-75.4485799227,\"stars\":3.0,\"review_count\":138,\"is_open\":1,\"attributes\":{\"RestaurantsGoodForGroups\":\"True\",\"RestaurantsAttire\":\"u'casual'\",\"RestaurantsReservations\":\"True\",\"NoiseLevel\":\"'average'\",\"Alcohol\":\"u'full_bar'\",\"RestaurantsTakeOut\":\"True\",\"BusinessAcceptsCreditCards\":\"True\",\"OutdoorSeating\":\"True\",\"RestaurantsPriceRange2\":\"2\",\"GoodForKids\":\"True\",\"Ambience\":\"{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'divey': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': True}\",\"HasTV\":\"True\",\"RestaurantsTableService\":\"True\",\"WiFi\":\"u'free'\",\"BikeParking\":\"True\",\"BusinessParking\":\"{'garage': False, 'street': False, 'validated': False, 'lot': True, 'valet': False}\",\"HappyHour\":\"True\",\"Caters\":\"False\",\"RestaurantsDelivery\":\"True\",\"GoodForMeal\":\"{u'breakfast': False, u'brunch': False, u'lunch': True, u'dinner': True, u'latenight': None, u'dessert': None}\"},\"categories\":\"Restaurants, American (New), American (Traditional), Pizza, Gluten-Free\",\"hours\":{\"Monday\":\"0:0-0:0\",\"Tuesday\":\"11:0-22:0\",\"Wednesday\":\"11:0-22:0\",\"Thursday\":\"11:0-22:0\",\"Friday\":\"11:0-23:0\",\"Saturday\":\"11:0-23:0\",\"Sunday\":\"11:0-22:0\"}}";
		String businessHourJson = "{\"business_id\":\"_rkjLrft2ZgHcLBLF9amXw\",\"Monday\":\"0:0-0:0\",\"Tuesday\":\"11:0-22:0\",\"Wednesday\":\"11:0-22:0\",\"Thursday\":\"11:0-22:0\",\"Friday\":\"11:0-23:0\",\"Saturday\":\"11:0-23:0\",\"Sunday\":\"11:0-22:0\"}";

		// business topic
		Business business = MelpAvroUtils.jsonToBusinessAvro(businessJson);
		businessTopic.pipeInput("_rkjLrft2ZgHcLBLF9amXw", business);

		// business_hour topic
		Business_hour businessHour = MelpAvroUtils
				.jsonToBusinessHourAvro(businessHourJson);
		businessHourTopic.pipeInput("_rkjLrft2ZgHcLBLF9amXw", businessHour);

		TestOutputTopic<String, Business_ES> outputTopic = testDriver
				.createOutputTopic("target", new StringDeserializer(),
						avroDeserializer);
		Business_ES outputBusiness = outputTopic.readValue();
		assertEquals(business.getBusinessId(), outputBusiness.getBusinessId());
		assertEquals(business.getName(), outputBusiness.getName());
		assertEquals(business.getCategories(), outputBusiness.getCategories());
		assertEquals(businessHour.getBusinessId(),
				outputBusiness.getBusinessId());
		assertEquals(businessHour.getMonday(),
				outputBusiness.getHours().getMonday());
		assertEquals(businessHour.getSunday(),
				outputBusiness.getHours().getSunday());
		assertEquals(business.getLatitude(),
				outputBusiness.getLocation().getLat());
		assertEquals(business.getLongitude(),
				outputBusiness.getLocation().getLon());
	}

	@AfterEach
	public void cleanUp() {
		testDriver.close();
	}
}
