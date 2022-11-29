package org.neo4j.tool.dto;

import lombok.Value;

/** Use an object for extensibility. */
@Value(staticConstructor = "of")
public class ConstraintStatus {
    boolean online;
}
