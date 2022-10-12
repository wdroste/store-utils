package org.neo4j.tool;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import picocli.CommandLine.Option;

abstract class AbstractIndexCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexCommand.class);

    @Option(
        defaultValue = "true",
        names = {"-n", "--no_auth"},
        description = "No authentication.")
    protected boolean noAuth;

    @Option(
        names = {"-a", "--url"},
        description = "Neo4j URL",
        defaultValue = "${NEO4J_URL:-bolt://localhost:7687}")
    protected String uri;

    @Option(
        names = {"-u", "--username"},
        description = "Neo4j Username",
        defaultValue = "${NEO4J_USERNAME}")
    protected String username;

    @Option(
        names = {"-p", "--password"},
        description = "Neo4j Password",
        defaultValue = "${NEO4J_PASSWORD}")
    protected String password;


    @Override
    public void run() {
        try (final var driver = buildDriver(uri, username, password, noAuth)) {
            execute(driver);
        }
    }

    abstract void execute(Driver driver);

    private Driver buildDriver(String uri, String username, String password, boolean noAuth) {
        // create the driver
        for (int i = 0; i < 5; i++) {
            try {
                final var config = Config.defaultConfig();
                if (noAuth) {
                    return GraphDatabase.driver(uri, config);
                }
                final var token = AuthTokens.basic(username, password);
                return GraphDatabase.driver(uri, token, config);
            }
            catch (ServiceUnavailableException ex) {
                LOG.error("Failed to connect retrying..");
            }
        }
        throw new IllegalStateException("Unable to connect to Neo4J: " + uri);
    }

    static IndexData fromRecord(Record record) {
        return new IndexData(
            record.get(0).asLong(),
            record.get(1).asString(),
            record.get(2).asString(),
            record.get(3).asFloat(),
            "UNIQUE".equals(record.get(4).asString()),
            record.get(5).asString(),
            record.get(6).asString(),
            toList(record.get(7)),
            toList(record.get(8)),
            record.get(9).asString()
        );
    }

    static List<String> toList(Value value) {
        return (null == value || value.isNull()) ? List.of() : value.asList(Value::asString);
    }
}
