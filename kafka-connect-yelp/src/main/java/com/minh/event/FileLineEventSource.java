package com.minh.event;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public abstract class FileLineEventSource implements IEventSource {
	
	private final File file;
    private Long count = 0L;

	
	public FileLineEventSource(final File file, final int offset) {
		this.file = file;
	}

	@Override
	public CompletableFuture<Long> start() throws Exception {
	    CompletableFuture<Long> completableFuture = new CompletableFuture<>();

	    Executors.newCachedThreadPool().submit(() -> {
	    	Files.lines(file.toPath()).forEach(a -> onEvent(a));
	        completableFuture.complete(count);
	        return null;
	    });
		
		return completableFuture;
	}

    public abstract void onEvent(final String event);
}
