package com.brinqa.tool.neo4j.job;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.neo4j.driver.Driver;

/**
 * Dumps the database to the specified location.
 * <p>
 * This job should remember where it left off and continue.
 */
@RequiredArgsConstructor
public class DatabaseDumpJob implements Runnable {
    private final Driver driver;
    private final DatabaseDumpRequest request;
    private final DatabaseDumpJobListener listener;

    enum Stage {
        READ_LABELS, DUMP_DATA, DUMP_RELATIONSHIPS
    }

    @Value
    static class State {
        Stage stage;
        long lastKey;
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        // find all the labels
        // dump all the data
        // dump all the relationships
    }
}
