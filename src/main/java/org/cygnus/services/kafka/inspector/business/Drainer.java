package org.cygnus.services.kafka.inspector.business;

public interface Drainer {

    Result drain(final String topicName);
}
