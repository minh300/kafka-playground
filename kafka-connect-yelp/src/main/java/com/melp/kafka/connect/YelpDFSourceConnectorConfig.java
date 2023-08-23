package com.melp.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;


public class YelpDFSourceConnectorConfig extends AbstractConfig {

    public static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_DOC = "Topic to write to";

    public static final String PATH_CONFIG = "path";
    private static final String PATH_DOC = "The path to the yelp business data files";

    public YelpDFSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public YelpDFSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(PATH_CONFIG, Type.STRING, Importance.HIGH, PATH_DOC);
    }

    public String getPath() {
        return this.getString(PATH_CONFIG);
    }

    public String getTopic() {
        return this.getString(TOPIC_CONFIG);
    }
}
