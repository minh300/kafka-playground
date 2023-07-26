package minh.event;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.launchdarkly.eventsource.MessageEvent;

public interface IEventSource {
	public CompletableFuture<Long> start(final Duration reconnectTime) throws Exception;
	
	public void shutdown() throws Exception;
	
	public void onEvent(final String event, final MessageEvent messageEvent);
	
	public void onClosed();

}
