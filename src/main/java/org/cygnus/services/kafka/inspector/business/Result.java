package org.cygnus.services.kafka.inspector.business;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class Result {

    private Long totalRecords;

    private Long duration;

    private Double tps;

}
