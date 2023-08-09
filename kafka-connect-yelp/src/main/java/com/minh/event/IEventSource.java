package com.minh.event;

import java.util.concurrent.CompletableFuture;


public interface IEventSource {
	public CompletableFuture<Long> start() throws Exception;
		
	public void onEvent(final String event);
}
