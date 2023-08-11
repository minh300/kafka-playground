package com.minh.event.processor;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.minh.kafka.connect.schema.YelpConnectSchemas.*;

public class BusinessProcessor implements IEventProccesor {
    public static final String BUSINESS_TOPIC = "business";
    public static final String BUSINESS_HOUR_TOPIC = "business_hour";
	private static final ObjectMapper mapper = new ObjectMapper();

	public void process(final String event, final int offset, BlockingQueue<SourceRecord> queue) throws Exception {
		JsonNode eventAsJsonNode = mapper.readTree(event);
		queue.add(new SourceRecord(
				Collections.singletonMap(SOURCE_STR, BUSINESS_TOPIC),
				Collections.singletonMap(OFFSET_STR, offset),
				BUSINESS_TOPIC, 
				null, 
				Schema.STRING_SCHEMA,
				eventAsJsonNode.get(BUSINESS_ID_FIELD).asText(),
				BUSINESS_SCHEMA,
				buildBusinessStruct(eventAsJsonNode),
				System.currentTimeMillis()));
		
		queue.add(new SourceRecord(
				Collections.singletonMap(SOURCE_STR, BUSINESS_HOUR_TOPIC),
				Collections.singletonMap(OFFSET_STR, offset),
				BUSINESS_HOUR_TOPIC, 
				null, 
				Schema.STRING_SCHEMA,
				eventAsJsonNode.get(BUSINESS_ID_FIELD).asText(),
				HOURS_SCHEMA,
				buildHoursStruct(eventAsJsonNode),
				System.currentTimeMillis()));		
	}

}
