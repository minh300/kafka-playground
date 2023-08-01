package minh.kafka.producer;


import com.launchdarkly.eventsource.MessageEvent;

import minh.event.AEventSource;
import minh.event.IEventSource;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class WikimediaTask extends SourceTask {

    private IEventSource processor;
    private BlockingQueue<SourceRecord> queue;


    /**
     * will use a static version for now
     */
    @Override
    public String version() {
        return WikimediaSource.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {

        //get all configs and store it in local variables
        String topic = map.get(WikimediaSource.TOPIC_CONFIG);
        String uri = map.get(WikimediaSource.URL_CONFIG);
        Duration reconnectDuration = Duration.ofMillis(Integer.parseInt(map.get(WikimediaSource.RECONNECT_TIME_CONFIG)));

        // initiate a new processor
        processor = new AEventSource(uri) {
            @Override
			public void onClosed() {

            }

            @Override
            public void onEvent(String event, MessageEvent messageEvent) {
                //each event will be added to stash
                queue.add(new SourceRecord(
                        Collections.singletonMap("source", "wikimedia"),
                        Collections.singletonMap("offset", 0),
                        topic,
                        Schema.STRING_SCHEMA,
                        messageEvent.getData()
                ));
            }
        };

        //start the processor
        try {
            queue = new LinkedBlockingDeque<>();
            processor.start(reconnectDuration);
        } catch (Exception e) {
            throw new ConnectException("unable to start processor",e);
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

        return records;
    }

    @Override
    public void stop() {

        // stop the processor on shutdown
        if (processor != null) {
            try {
                processor.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}