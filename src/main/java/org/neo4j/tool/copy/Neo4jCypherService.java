package org.neo4j.tool.copy;

import io.reactivex.rxjava3.core.Flowable;
import org.neo4j.driver.Record;
import org.neo4j.tool.copy.FlowableRetryUtil.Config;

/** Cypher Service from Compute to optimize queries to Neo4j. */
public interface Neo4jCypherService {
  Flowable<Record> readAsync(Config config, CypherQueryContainer query);
}
