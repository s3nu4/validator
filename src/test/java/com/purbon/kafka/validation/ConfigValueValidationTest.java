package com.purbon.kafka.validation;

import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ConfigValueValidationTest {

    @Test
    public void shouldVerifyDifferentValuesSuccessfullyWithError() throws ConfigurationException {
        Map<String, String> config = new HashMap<>();
        config.put("replication.factor", "34");
        config.put("num.partitions", "123");

        Topic topic = new Topic("topic", config);

        Assertions.assertThrows(ValidationException.class, () -> {
            ConfigValueValidation validation = new ConfigValueValidation("num.partitions", "234");
            validation.valid(topic);
        }, "Topic topic, config: num.partitions = 123, different from expected value 234");
    }

    @Test
    public void shouldVerifyEqualValuesSuccessfully() throws ConfigurationException, ValidationException {
        Map<String, String> config = new HashMap<>();
        config.put("replication.factor", "34");
        config.put("num.partitions", "123");

        Topic topic = new Topic("topic", config);

        ConfigValueValidation validation = new ConfigValueValidation("num.partitions", "123");
        validation.valid(topic);
    }
}
