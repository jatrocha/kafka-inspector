package org.cygnus.services.kafka.inspector;

import org.cygnus.services.kafka.inspector.business.Counter;
import org.cygnus.services.kafka.inspector.business.Drainer;
import org.cygnus.services.kafka.inspector.business.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.shell.jline.InteractiveShellApplicationRunner;
import org.springframework.shell.jline.ScriptShellApplicationRunner;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = {
        InteractiveShellApplicationRunner.SPRING_SHELL_INTERACTIVE_ENABLED + "=false",
        ScriptShellApplicationRunner.SPRING_SHELL_SCRIPT_ENABLED + "=false"
})
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class KafkaInspectorApplicationTests {

    private static final String TOPIC_NAME = "test";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Counter counter;

    @Autowired
    private Drainer drainer;

    @Autowired
    private Commands commands;

    @BeforeEach
    void setup() {

        for (int i = 0; i < 1000; i++) {

            kafkaTemplate.send(TOPIC_NAME, "foo-" + i, "bar-" + i);
        }

    }

    @Test
    void shouldExecuteCountCommand() {

        assertNotNull(commands.count(TOPIC_NAME));
    }

    @Test
    void shouldExecuteDrainCommand() {

        assertNotNull(commands.drain(TOPIC_NAME));
    }

    @Test
    void shouldCountTotalRecordsInTopic() {

        final Result expected = Result.builder().totalRecords(1000L).build();

        final Result actual = counter.countRecords(TOPIC_NAME);

        assertEquals(expected.getTotalRecords(), actual.getTotalRecords());
    }

    @Test
    void shouldDrainTopic() {

        final Result expected = Result.builder().totalRecords(1000L).build();

        final Result actual = drainer.drain(TOPIC_NAME);

        assertEquals(expected.getTotalRecords(), actual.getTotalRecords());
    }

}
