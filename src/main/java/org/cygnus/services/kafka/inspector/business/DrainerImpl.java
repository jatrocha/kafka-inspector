package org.cygnus.services.kafka.inspector.business;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
public class DrainerImpl extends BaseKafkaInspector implements Drainer {

    @Override
    public Result drain(final String topicName) {

        final Result result = super.execute(topicName, getProperties());

        log.info(String.format("Counting records on topic %s: %s", topicName, result));

        return result;
    }

    protected Properties getProperties() {

        Properties properties = super.getProperties();

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        return properties;
    }
}
