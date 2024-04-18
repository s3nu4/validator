package com.purbon.kafka.validation;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.Consumer;
import com.purbon.kafka.topology.validation.TopologyValidation;
import com.typesafe.config.ConfigException;

public class ConsumerGroupValidation implements TopologyValidation {

    private static final String TOPOLOGY_CONSUMER_FORBIDDEN_GROUP_FIELD = "*";
    private Configuration config;
    private static final String TOPOLOGY_CONSUMER_FORBIDDEN_GROUP_FIELD_SKIP = "validations.consumer.group.skip";

    public ConsumerGroupValidation(Configuration config) {
        this.config = config;
    }

    @Override
    public void valid(Topology topology) throws ValidationException {
        if (getTForbiddenGroupEnabledConfig(this.config)) {
            return;
        }
        for (Project proj : topology.getProjects()) {
            for (Topic topic : proj.getTopics()) {
                for (Consumer consumer : topic.getConsumers()) {
                    if (!consumer.getPrincipal().isBlank() && consumer.getGroup().isEmpty()) {
                        throw new ValidationException("Group id is not set or is set to *");
                    }
                    if (consumer.getGroup().isPresent() && consumer.getGroup().get().equals(TOPOLOGY_CONSUMER_FORBIDDEN_GROUP_FIELD)) {
                        throw new ValidationException("Group id is not set or is set to *");
                    }
                }
            }
        }
    }

    private static boolean getTForbiddenGroupEnabledConfig(Configuration config) {
        try {
            return Boolean.parseBoolean(config.getProperty(TOPOLOGY_CONSUMER_FORBIDDEN_GROUP_FIELD_SKIP));
        } catch (ConfigException ce) {
            return false;
        }
    }
}
