package org.cygnus.services.kafka.inspector.business;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
public class CounterImpl extends BaseKafkaInspector implements Counter {

    @Override
    public Result countRecords(final String topicName) {

        final Result result = super.execute(topicName, getProperties());

        log.info(String.format("Counting records on topic %s: %s", topicName, result));

        return result;
    }

    protected Properties getProperties() {

        Properties properties = super.getProperties();

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }
}
