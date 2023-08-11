package com.minh.kafka.connect;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class YelpDFSourceConnector extends SourceConnector {
    public static String VERSION = "0.0.1";
    public static final String TOPIC_CONFIG = "topic";    
    public static final String PATH_CONFIG = "path";
    
    //config definition object
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(PATH_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The path of the folder of the data file");

    private String uri;
    private String topic;

	@Override
	public String version() {
        return VERSION;
	}

	@Override
	public void start(Map<String, String> props) {
        // init the configs on start
        topic = props.get(TOPIC_CONFIG);
        uri = props.get(PATH_CONFIG);
	}

	@Override
	public Class<? extends Task> taskClass() {
        return YelpDFTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> config = new HashMap<String,String>();
        config.put(TOPIC_CONFIG, topic);
        config.put(PATH_CONFIG, uri);
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
