package org.neo4j.tool.dto;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder(toBuilder = true)
public class IndexData {
    long id;
    String name;
    String state;
    float populationPercent;
    boolean uniqueness;
    String type;
    List<String> labelsOrTypes;
    List<String> properties;
    String indexProvider;
}
