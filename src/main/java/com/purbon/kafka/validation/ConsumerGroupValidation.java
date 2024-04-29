package com.purbon.kafka.validation;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.model.users.Consumer;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistryInstance;
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
        checkProjects(topology);
        checkPlatform(topology);
    }

    private void checkPlatform(Topology topology) throws ValidationException {
        for (SchemaRegistryInstance instance : topology.getPlatform().getSchemaRegistry().getInstances()) {
            if (instance.groupString().equals(TOPOLOGY_CONSUMER_FORBIDDEN_GROUP_FIELD)) {
                throw new ValidationException(String.format("Group id for Schema Registry %s is not set or is set to *", instance.groupString()));
            }
        }
    }

    private void checkProjects(Topology topology) throws ValidationException {
        for (Project proj : topology.getProjects()) {
            for (Consumer consumer : proj.getConsumers()) {
                checkGroup(consumer);
            }
            for (Topic topic : proj.getTopics()) {
                for (Consumer consumer : topic.getConsumers()) {
                    checkGroup(consumer);
                }
            }
            for (Connector con : proj.getConnectors()) {
                if (con.getGroup().isPresent() && con.getGroup().get().equals("*")) {
                    throw new ValidationException(String.format("Group id for Connector %s is not set or is set to *", con.getPrincipal()));
                }
            }
        }
    }

    private static void checkGroup(Consumer consumer) throws ValidationException {
        if (!consumer.getPrincipal().isBlank() && consumer.getGroup().isEmpty()) {
            throw new ValidationException(String.format("Group id for consumer %s is not set or is set to *", consumer.getPrincipal()));
        }
        if (consumer.getGroup().isPresent() && consumer.getGroup().get().equals(TOPOLOGY_CONSUMER_FORBIDDEN_GROUP_FIELD)) {
            throw new ValidationException(String.format("Group id for consumer %s is not set or is set to *", consumer.getPrincipal()));
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
