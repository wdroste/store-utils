package com.brinqa.tool.neo4j.service;

import org.neo4j.driver.Driver;

import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Determine all the labels for this database.
 */
public class AllLabelsService {

    /**
     * Determine all the labels in the database.
     *
     * @return unique set of labels.
     */
    public Set<String> determineAllLabels(Driver driver) {
        try (final var session = driver.session()) {
            return session.executeRead(ctx -> {
                final var result = ctx.run("CALL db.labels();");
                return result.stream().map(r -> r.get(0).asString()).collect(toUnmodifiableSet());
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
