package com.melp.checkin.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Mono;
import com.melp.avro.Location;

@Service
public class CheckinServiceImpl implements CheckinService {
 
	private final KafkaProducer<String, Location> producer;
	private final String topic;
	
	public CheckinServiceImpl(final KafkaProducer<String, Location> producer, final String topic) {
		this.producer = producer;
		this.topic = topic;
	}
	
	@Override
	public Mono<Void> checkin(final float latitude, final float longitude) {
		Location loc = new Location(latitude, longitude);
		producer.send(new ProducerRecord<String, Location>(topic, loc));
		return Mono.empty().then();
	}

}
