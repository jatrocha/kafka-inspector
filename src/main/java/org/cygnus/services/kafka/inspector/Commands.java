package org.cygnus.services.kafka.inspector;

import org.cygnus.services.kafka.inspector.business.Counter;
import org.cygnus.services.kafka.inspector.business.Drainer;
import org.cygnus.services.kafka.inspector.business.Result;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

@ShellComponent()
public class Commands {

//    @ShellMethod(value = "Get to total records on a given topic", key = "count")
//    public String totalMessages(final String topicName) {
//
//        final Properties properties = getProperties();
//
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//        Long total = 0L;
//
//        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
//
//            consumer.subscribe(Arrays.asList(topicName));
//
//            Set<TopicPartition> assignment;
//
//            while ((assignment = consumer.assignment()).isEmpty()) {
//
//                consumer.poll(Duration.ofMillis(100));
//            }
//
//            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
//
//            final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(assignment);
//
//            assert (endOffsets.size() == beginningOffsets.size());
//
//            assert (endOffsets.keySet().equals(beginningOffsets.keySet()));
//
//            total = beginningOffsets.entrySet().stream().mapToLong(entry -> {
//
//                TopicPartition tp = entry.getKey();
//
//                Long beginningOffset = entry.getValue();
//
//                Long endOffset = endOffsets.get(tp);
//
//                return endOffset - beginningOffset;
//            }).sum();
//
//        }
//
//        return String.format("Found %s records on topic %s", total, topicName);
//
//    }

    private Counter counter;

    private Drainer drainer;

    public Commands(Counter counter, Drainer drainer) {

        this.counter = counter;

        this.drainer = drainer;
    }

    @ShellMethod(value = "Get to total records on a given topic", key = "count")
    public String count(final String topicName) {

        final Result result = counter.countRecords(topicName);

        return String.format("Found %s records on topic %s in %s seconds (%s TPS)",
                result.getTotalRecords(),
                topicName,
                result.getDuration(),
                result.getTps());

    }

    @ShellMethod(value = "Drain all records from a given topic", key = "drain")
    public String drain(final String topicName) {

        final Result result = drainer.drain(topicName);

        return String.format("%s records drained from topic %s in %s seconds (%s TPS)",
                result.getTotalRecords(),
                topicName,
                result.getDuration(),
                result.getTps());
    }

}
