package org.cygnus.services.kafka.inspector.business;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@Component
public abstract class BaseKafkaInspector {

    @Autowired
    private Environment environment;

    protected Result execute(final String topicName, final Properties properties) {

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topicName));

        AtomicReference<Long> total = new AtomicReference<>(0L);

        final int retries = Integer.parseInt(environment.getProperty("retries"));

        int noRecordsCount = 0;

        final Long start = System.currentTimeMillis();

        while (true) {

            final int pollDurationInSeconds = Integer.parseInt(environment.getProperty("pollDurationInSeconds"));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollDurationInSeconds));

            if (records.count() == 0) {

                noRecordsCount++;

                if (noRecordsCount > retries) break;

                else continue;
            }

            records.forEach(record -> {

                total.getAndSet(total.get() + 1);
            });

        }

        final Long duration = (System.currentTimeMillis() - start) / 1000;

        Double tps = (double) total.get() / (double) duration;

        consumer.close();

        return Result.builder().totalRecords(total.get()).duration(duration).tps(tps).build();
    }

    protected Properties getProperties() {

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("bootstrap.servers"));

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("group.id"));

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
