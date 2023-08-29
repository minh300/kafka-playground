package com.melp.kafka.elasticsearch.transformer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.melp.avro.Business;
import com.melp.avro.Business_ES;
import com.melp.avro.Business_hour;
import com.melp.avro.Business_hour_ES;
import com.melp.avro.Location;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class ESTransformerKafkaStream {
	private static final Logger log = LoggerFactory.getLogger(ESTransformerKafkaStream.class);

	private final String businessTopic;
	private final String businessHourTopic;
	private final String targetTopic;
	private final String registryUrl;
    private final SchemaRegistryClient schemaRegistryClient;

	public ESTransformerKafkaStream(final String businessTopic,
			final String businessHourTopic, final String targetTopic,
			final SchemaRegistryClient schemaRegistryClient, final String registryUrl) {
		this.businessTopic = businessTopic;
		this.businessHourTopic = businessHourTopic;
		this.targetTopic = targetTopic;
		this.schemaRegistryClient = schemaRegistryClient;
		this.registryUrl = registryUrl;
	}

	protected Topology createTopology() {
		final Map<String, String> serdeConfig = Collections
				.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
		
		final Serde<Business> businessSpecificAvroSerde = schemaRegistryClient !=null ? new SpecificAvroSerde<>(schemaRegistryClient) : new SpecificAvroSerde<>();
		businessSpecificAvroSerde.configure(serdeConfig, false);
		
		final Serde<Business_hour> businessHourSpecificAvroSerde = schemaRegistryClient !=null ? new SpecificAvroSerde<>(schemaRegistryClient) : new SpecificAvroSerde<>();
		businessHourSpecificAvroSerde.configure(serdeConfig, false);
		
		StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, Business> businessStream = builder
				.stream(businessTopic);
		KStream<String, Business_hour> businessHourStream = builder
				.stream(businessHourTopic);

		businessStream.join(businessHourStream, (business, business_hour) -> {
			Business_hour_ES converetedHours = new Business_hour_ES(
					business_hour.getMonday(), business_hour.getTuesday(),
					business_hour.getWednesday(), business_hour.getThursday(),
					business_hour.getFriday(), business_hour.getSaturday(),
					business_hour.getSunday());
			Business_ES convertedBusiness = new Business_ES(
					business.getBusinessId(), business.getName(),
					business.getAddress(), business.getCity(),
					business.getState(), business.getPostalCode(),
					business.getStars(), business.getReviewCount(),
					business.getIsOpen(), business.getCategories(),
					converetedHours, new Location(business.getLatitude(),
							business.getLongitude()));
			log.info("Created new Business: "+ convertedBusiness.getBusinessId());
			return convertedBusiness;
		}, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(15)),
				StreamJoined.with(Serdes.String(), businessSpecificAvroSerde,
						businessHourSpecificAvroSerde))
		.to(targetTopic);
		return builder.build();
	}

	public void start(final Properties config) {
		Topology topology = createTopology();
		KafkaStreams streams = new KafkaStreams(topology, config);
	    final CountDownLatch latch = new CountDownLatch(1);
	    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
	        @Override
	        public void run() {
	            streams.close(Duration.ofMillis(30000));
	            latch.countDown();
	        }
	    });
	    try {
	    	//TODO
//	    	streams.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler() {
//
//				@Override
//				public StreamThreadExceptionResponse handle(
//						Throwable exception) {
//	                streams.cleanUp();
//					return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
//				}
//	    		
//	    	});

	        streams.start();
	        latch.await();
	    } catch (Throwable e) {
	        //logger.error("Stream stopped with error");
	        System.exit(1);
	    }
	}
}
