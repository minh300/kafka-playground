package com.melp.event.processor;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.melp.kafka.connect.schema.YelpConnectSchemas.*;

public class BusinessProcessor implements IEventProccesor {
	private static final Logger log = LoggerFactory.getLogger(BusinessProcessor.class);

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
		Struct hours = buildHoursStruct(eventAsJsonNode);
		if(hours!=null) {
			queue.add(new SourceRecord(
					Collections.singletonMap(SOURCE_STR, BUSINESS_HOUR_TOPIC),
					Collections.singletonMap(OFFSET_STR, offset),
					BUSINESS_HOUR_TOPIC, 
					null, 
					Schema.STRING_SCHEMA,
					eventAsJsonNode.get(BUSINESS_ID_FIELD).asText(),
					HOURS_SCHEMA,
					hours,
					System.currentTimeMillis()));
		}
		Thread.sleep(2000);
	}

}
