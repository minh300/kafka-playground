package minh.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;

import minh.common.AppProperties;
import minh.event.IEventSource;
import minh.event.UrlEventSource;
import minh.event.KafkaEventHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class WikimediaSource extends SourceConnector {
    public static String VERSION = "0.0.1";
    public static final String TOPIC_CONFIG = "topic";
    public static final String URL_CONFIG = "url";
    public static final String RECONNECT_TIME_CONFIG = "reconnect.duration";
    
    //config definition object
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(URL_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The event stream url to fetch events from")
            .define(RECONNECT_TIME_CONFIG, ConfigDef.Type.INT, null, ConfigDef.Importance.LOW, "reconnect duration (milliseconds) config for event stream url. defaults to 3 seconds");

    private String topic;
    private String uri;
    private String reconnectDuration;

	@Override
	public String version() {
        return VERSION;
	}

	@Override
	public void start(Map<String, String> props) {
        // init the configs on start
        topic = props.get(WikimediaSource.TOPIC_CONFIG);
        uri = props.get(WikimediaSource.URL_CONFIG);
        reconnectDuration = props.getOrDefault(WikimediaSource.RECONNECT_TIME_CONFIG, "3000");
	}

	@Override
	public Class<? extends Task> taskClass() {
        return WikimediaTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> config = new HashMap<String,String>();
        config.put(TOPIC_CONFIG, topic);
        config.put(URL_CONFIG, uri);
        config.put(RECONNECT_TIME_CONFIG, reconnectDuration);
        return Collections.singletonList(config);
	}

	@Override
	public void stop() {		
	}

	@Override
	public ConfigDef config() {
        return CONFIG_DEF;
	}
}
