package org.neo4j.tool;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class IndexData {
    long id;
    String name;
    String state;
    float populationPercent;
    boolean uniqueness;
    String type;
    String entityType;
    List<String> labelsOrTypes;
    List<String> properties;
    String indexProvider;
}
