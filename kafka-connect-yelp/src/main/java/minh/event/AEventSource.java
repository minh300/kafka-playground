package minh.event;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

public abstract class AEventSource implements IEventSource {
	AEventSource _eventProcessor = this;

    CompletableFuture<Long> promise;

    private Long count = 0L;

    private final String uri;
    
    private BackgroundEventSource eventSource;
    
    public AEventSource(final String uri) {
        this.uri = uri;
    }

    /**
     * override this method to do on close callback
     */
    public abstract void onClosed();


    /**
     * override this method to handle each message
     */
    public abstract void onEvent(String event, MessageEvent messageEvent);

    /**
     * shuts down the processor
     * @throws Exception throw if the processor hasn't started
     */
    public void shutdown() throws Exception {
        if (eventSource == null) {
            promise.complete(count);
            throw new Exception("trying to stop EventProcessor without starting");
        } else {
            eventSource.close();
        }
    }

    /**
     * start the processor
     * @param reconnectTime reconnect if processor in case of error for this duration
     * @throws Exception throws if already started
     * @return a promise which the caller can wait on ; this promise will complete once the processor is shutdown
     */
    public CompletableFuture<Long> start(Duration reconnectTime) throws Exception {
        if (eventSource != null) {
            throw new Exception("trying to start already started EventProcessor");
        }
        if (reconnectTime == null) {
            reconnectTime = Duration.ofMillis(3000);
        }
        EventSource.Builder builder = new EventSource.Builder(URI.create(uri));
        BackgroundEventSource.Builder backGroundEventSourcebuilder = new BackgroundEventSource.Builder(new CustomEventHandler(), builder);
        this.eventSource = backGroundEventSourcebuilder.build();
        promise = new CompletableFuture<>();

        eventSource.start();

        return promise;

    }
    
    /**
     * a custom event handler that delegates to processor methods
     */
    public class CustomEventHandler implements BackgroundEventHandler {

        @Override
        public void onOpen() { }

        @Override
        public void onClosed() {
            _eventProcessor.onClosed();
            promise.complete(count);
        }

        @Override
        public void onMessage(String event, MessageEvent messageEvent) {
            count++;
            onEvent(event, messageEvent);
        }

        @Override
        public void onComment(String comment) {}

        @Override
        public void onError(Throwable t) {
            System.out.println("onError");
            t.printStackTrace();
        }

    }
}
