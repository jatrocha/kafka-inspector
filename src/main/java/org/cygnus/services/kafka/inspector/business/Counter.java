package org.cygnus.services.kafka.inspector.business;

public interface Counter {

    Result countRecords(final String topicName);

}
