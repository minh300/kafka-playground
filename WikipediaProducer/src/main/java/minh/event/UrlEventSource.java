package minh.event;

import java.net.URI;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

public class UrlEventSource implements IEventSource {

	private final BackgroundEventSource eventSource;
	
	public UrlEventSource(final String url, final BackgroundEventHandler handler) {
        EventSource.Builder builder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder backGroundEventSourcebuilder = new BackgroundEventSource.Builder(handler, builder);
        this.eventSource = backGroundEventSourcebuilder.build();
	}

	@Override
	public void start() {
		eventSource.start();
	}

}
