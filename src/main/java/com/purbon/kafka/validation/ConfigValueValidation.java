package com.purbon.kafka.validation;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.validation.TopicValidation;

import java.util.Map;

public class ConfigValueValidation implements TopicValidation {

    private static final String TOPOLOGY_TOPIC_CONFIG_VALIDATION_FIELD = "topology.topic.config.validation.field";
    private static final String TOPOLOGY_TOPIC_CONFIG_VALIDATION_VALUE = "topology.topic.config.validation.value";

    private final String field;
    private final String value;
    private Configuration config;

    @Override
    public void valid(Topic topic) throws ValidationException {

        Map<String, String> topicConfig = topic.getConfig();
        if (!topicConfig.containsKey(field)) {
            throw new ValidationException(String.format(
                    "Topic %s, does not has the config value %s", topic.getName(), field
            ));
        }
        if (!topicConfig.get(field).equals(value)) {
            throw new ValidationException(String.format(
                    "Topic %s, config: %s = %s, different from expected value %s",
                    topic.getName(), field, topicConfig.get(field), value
            ));
        }
    }

    public ConfigValueValidation(Configuration config) {
        this(getField(config), getValue(config));
        this.config = config;
    }

    public ConfigValueValidation(String field, String value) {
        this.field = field;
        this.value = value;
    }

    private static String getValue(Configuration config) {
        return config.getProperty(TOPOLOGY_TOPIC_CONFIG_VALIDATION_VALUE);
    }

    private static String getField(Configuration config) {
        return config.getProperty(TOPOLOGY_TOPIC_CONFIG_VALIDATION_FIELD);
    }

}
