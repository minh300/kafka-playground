package com.melp.event.processor;

import java.util.concurrent.BlockingQueue;

import org.apache.kafka.connect.source.SourceRecord;

public interface IEventProccesor {
	public static final String OFFSET_STR = "offset";
	public static final String SOURCE_STR = "source";
	public void process(final String event, final int offset, BlockingQueue<SourceRecord> queue)  throws Exception;
}
