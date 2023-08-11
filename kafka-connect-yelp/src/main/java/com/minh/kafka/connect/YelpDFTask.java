package com.minh.kafka.connect;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minh.event.FileLineEventSource;
import com.minh.event.IEventSource;
import com.minh.event.processor.IEventProccesor;
import com.minh.event.processor.ProcessorUtil;
import com.minh.kafka.connect.schema.YelpConnectSchemas;
import com.minh.kafka.connect.util.VersionUtil;

public class YelpDFTask extends SourceTask {
	private static final Logger log = LoggerFactory.getLogger(YelpDFTask.class);
	private static final String OFFSET_STR = "offset";
	private static final String SOURCE_STR = "source";
	private static final String YELP_STR = "yelp";
	public YelpDFSourceConnectorConfig config;
	private IEventSource processor;
	private BlockingQueue<SourceRecord> queue;
	private int offset;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		// Do things here that are required to start your task. This could be open a
		// connection to a database, etc.
		config = new YelpDFSourceConnectorConfig(map);
		queue = new LinkedBlockingDeque<>();
		offset = getOffset();
		startFileReader();
	}

	private void startFileReader() {
		String topic = config.getTopic();
		String path = config.getPath();
		// TODO batch size customization
		File dfSet = new File(path, topic);
		if (dfSet.exists()) {
			processor = new FileLineEventSource(new File(path, topic), offset) {
				@Override
				public void onEvent(final String event) {
					IEventProccesor processor = ProcessorUtil.fromString(topic);
					try {
						processor.process(event, offset, queue);
						offset++;
					} catch (Exception e) {
						log.error("Unable to read event: " + event, e);
					}
				}
			};
			try {
				processor.start();
			} catch (Exception e) {
				throw new ConnectException("unable to start processor", e);
			}
		}
	}

	private int getOffset() {
		Map<String, Object> lastSourceOffset = null;
		lastSourceOffset = context.offsetStorageReader().offset(sourcePartition());
		if (lastSourceOffset == null) {
			return 0;
		} else {
			return (int) lastSourceOffset.get(OFFSET_STR);
		}
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> records = new LinkedList<>();

		SourceRecord event = queue.poll(1L, TimeUnit.SECONDS);
		if (event == null) {
			return records;
		}

		records.add(event);
		queue.drainTo(records);
		// TODO return null when done to stop task?
		return records;
	}

	private Map<String, String> sourcePartition() {
		return Collections.singletonMap(SOURCE_STR, YELP_STR);
	}

	@Override
	public synchronized void stop() {
		// Do whatever is required to stop your task.
	}



}