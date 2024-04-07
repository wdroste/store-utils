package org.neo4j.tool.copy;

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;

/**
 * Container for cypher query.
 */
@Value
@Jacksonized
@Builder(toBuilder = true)
public class CypherQueryContainer {

    /**
     * CypherQuery to use with Neo4j
     */
    @NonNull String query;

    /**
     * Parameters to pass with the query.
     */
    @Singular
    Map<String, Object> parameters;
}
