package com.purbon.kafka.validation;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Platform;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.model.users.Consumer;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistry;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistryInstance;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class ConfigValueValidationTest {

    Topology topology = new TopologyImpl();
    Project proj = new ProjectImpl();
    Topic topic = new Topic();
    List<Consumer> consumers = new ArrayList<Consumer>();
    Configuration conf = new Configuration();
    Connector conn = new Connector();
    Platform platform = new Platform();

    @BeforeEach
    public void beforeTest() {
        topology = new TopologyImpl();
        proj = new ProjectImpl("Project1");
        topic = new Topic("Topic1");
        consumers = new ArrayList<>();
        conf = new Configuration();
        conn = new Connector();
        platform = new Platform();
    }

    @Test
    public void shouldVerifyConsumersUnderTopicsWithError() throws ConfigurationException {
        consumers.add(new Consumer("consumer1", "*"));
        topic.setConsumers(consumers);
        List<Topic> topics = new ArrayList<>();
        topics.add(topic);
        proj.setTopics(topics);
        topology.addProject(proj);
        Assertions.assertThrows(ValidationException.class, () -> {
            ConsumerGroupValidation validation = new ConsumerGroupValidation(conf);
            validation.valid(topology);
        }, "Group is under topic level is set to '*', should raise an error");
    }

    @Test
    public void shouldVerifyConsumersUnderTopicsWithoutGroupButPrincipalWithError() throws ConfigurationException {
        consumers.add(new Consumer("consumer1"));
        topic.setConsumers(consumers);
        List<Topic> topics = new ArrayList<>();
        topics.add(topic);
        proj.setTopics(topics);
        topology.addProject(proj);
        Assertions.assertThrows(ValidationException.class, () -> {
            ConsumerGroupValidation validation = new ConsumerGroupValidation(conf);
            validation.valid(topology);
        }, "Group is under topic level is set to '*', should raise an error");
    }

    @Test
    public void shouldVerifyConsumersUnderProjectWithError() throws ConfigurationException {

        consumers.add(new Consumer("consumer1", "*"));
        proj.setConsumers(consumers);
        topology.addProject(proj);

        Assertions.assertThrows(ValidationException.class, () -> {
            ConsumerGroupValidation validation = new ConsumerGroupValidation(conf);
            validation.valid(topology);
        }, "Group under Project is set to '*', should raise an error");
    }

    @Test
    public void shouldVerifyConsumersUnderProjectWithoutGroupButPrincipalWithError() throws ConfigurationException {

        consumers.add(new Consumer("consumer1"));
        proj.setConsumers(consumers);
        topology.addProject(proj);

        Assertions.assertThrows(ValidationException.class, () -> {
            ConsumerGroupValidation validation = new ConsumerGroupValidation(conf);
            validation.valid(topology);
        }, "Group under Project is set to '*', should raise an error");
    }

    @Test
    public void shouldVerifyConsumersUnderConnectWithError() throws ConfigurationException {

        consumers.add(new Consumer("consumer1", "*"));
        conn.setGroup(Optional.of("*"));
        conn.setPrincipal("Con1");
        List<Connector> connectors = new ArrayList<>();
        connectors.add(conn);
        proj.setConnectors(connectors);
        topology.addProject(proj);

        Assertions.assertThrows(ValidationException.class, () -> {
            ConsumerGroupValidation validation = new ConsumerGroupValidation(conf);
            validation.valid(topology);
        }, "Group on Connect Level is set to '*', should raise an error");
    }

    @Test
    public void shouldVerifySchemaRegistryUnderPlatformWithError() throws ConfigurationException {

        consumers.add(new Consumer("consumer1", "*"));
        conn.setGroup(Optional.of("abc"));
        conn.setPrincipal("Con1");
        List<Connector> connectors = new ArrayList<>();
        connectors.add(conn);
        proj.setConnectors(connectors);
        SchemaRegistryInstance schemaRegistryInstance = new SchemaRegistryInstance("testprincial");
        schemaRegistryInstance.setGroup(Optional.of("*"));
        List<SchemaRegistryInstance> schemaRegistryInstances = new ArrayList<>();
        schemaRegistryInstances.add(schemaRegistryInstance);
        SchemaRegistry schemaRegistry = new SchemaRegistry();
        schemaRegistry.setInstances(schemaRegistryInstances);
        platform.setSchemaRegistry(schemaRegistry);
        topology.addProject(proj);
        topology.setPlatform(platform);

        Assertions.assertThrows(ValidationException.class, () -> {
            ConsumerGroupValidation validation = new ConsumerGroupValidation(conf);
            validation.valid(topology);
        }, "Group on Connect Level is set to '*', should raise an error");
    }
}
