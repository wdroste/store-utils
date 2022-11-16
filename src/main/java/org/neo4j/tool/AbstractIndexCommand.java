package org.neo4j.tool;

import static java.util.stream.Collectors.joining;
import static org.neo4j.tool.util.Print.println;

import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.tool.VersionQuery.Neo4jVersion;
import org.neo4j.tool.dto.IndexData;
import org.neo4j.tool.dto.IndexStatus;
import org.neo4j.tool.dto.IndexStatus.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

abstract class AbstractIndexCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexCommand.class);

    @Option(
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
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    abstract void execute(Driver driver) throws IOException;

    abstract String getFilename();

    private Driver buildDriver(String uri, String username, String password, boolean noAuth) {
        // create the driver
        for (int i = 0; i < 5; i++) {
            try {
                final var config = Config.defaultConfig();
                if (noAuth) {
                    println("Attempting to connect without authentication.");
                    return GraphDatabase.driver(uri, config);
                }
                println("Attempting to connect with basic authentication.");
                final var token = AuthTokens.basic(username, password);
                return GraphDatabase.driver(uri, token, config);
            } catch (ServiceUnavailableException ex) {
                LOG.error("Failed to connect retrying..");
            }
        }
        throw new IllegalStateException("Unable to connect to Neo4J: " + uri);
    }

    static IndexData fromRecord(Record record) {
        return IndexData.builder()
                .id(record.get(0).asLong())
                .name(record.get(1).asString())
                .state(record.get(2).asString())
                .populationPercent(record.get(3).asFloat())
                .uniqueness("UNIQUE".equalsIgnoreCase(record.get(4).asString()))
                .type(record.get(5).asString())
                .entityType(record.get(6).asString())
                .labelsOrTypes(toList(record.get(7)))
                .properties(toList(record.get(8)))
                .indexProvider(record.get(9).asString())
                .build();
    }

    static List<String> toList(Value value) {
        return (null == value || value.isNull()) ? List.of() : value.asList(Value::asString);
    }

    static void progressPercentage(int remain) {
        if (remain > 100) {
            throw new IllegalArgumentException();
        }
        int maxBareSize = 100; // 100 unit for 100%
        char defaultChar = '-';
        String icon = "*";
        String bare = new String(new char[maxBareSize]).replace('\0', defaultChar) + "]";
        String bareDone = "[" + icon.repeat(Math.max(0, remain));
        String bareRemain = bare.substring(remain);
        System.out.print("\r" + bareDone + bareRemain + " " + remain + "%");
        if (remain == 100) {
            System.out.print("\n");
        }
    }

    List<IndexData> readIndexesFromFilename() {
        return readIndexesFromFile(new File(this.getFilename()));
    }

    List<IndexData> readIndexesFromFile(File f) {
        final var ret = new ArrayList<IndexData>();
        final var gson = new GsonBuilder().create();
        try (final var rdr = new BufferedReader(new FileReader(f))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                final var index = gson.fromJson(line, IndexData.class);
                ret.add(index);
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return ret;
    }

    void createIndex(final Driver driver, Neo4jVersion version, IndexData index) {
        final var query = indexOrConstraintQuery(version, index);
        println(query);
        try {
            writeTransaction(driver, query);
        } catch (Throwable th) {
            LOG.error("Failed to create index: {}", query, th);
            throw new RuntimeException(th);
        }
    }

    boolean validIndex(final Driver driver, final IndexData index) {
        // insure index creation started
        for (int i = 0; i < 10; i++) {
            final var status = indexProgress(driver, index.getName());
            if (null != status && status.getState().isOk()) {
                return true;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    void createIndexWaitForCompletion(
            final Driver driver, final Neo4jVersion version, final IndexData index) {
        createIndex(driver, version, index);

        if (!validIndex(driver, index)) {
            println("Failed to create index: %s", index.getName());
            return;
        }

        // wait for completion
        int pct = 0;
        while (pct < 100) {
            progressPercentage(pct);
            final var status = indexProgress(driver, index.getName());
            if (status.getState().isFailed()) {
                println("%nFailed to create index: %s", index.getName());
                return;
            }
            pct = (int) status.getProgress();
            progressPercentage(pct);
        }
    }

    void writeTransaction(final Driver driver, final String query) {
        // query for all the indexes
        try (Session session = driver.session()) {
            assert session != null;
            ResultSummary resultSummary = session.writeTransaction(tx -> tx.run(query).consume());
            if (LOG.isDebugEnabled()) {
                LOG.debug(resultSummary.toString());
            }
        }
    }

    String createConstraintFormat(Neo4jVersion version) {
        switch (version) {
            case v4_2:
            case v4_3:
                return "CREATE CONSTRAINT `%s` IF NOT EXISTS ON (n:`%s`) ASSERT n.%s IS UNIQUE";
            case v4_4:
                return "CREATE CONSTRAINT `%s` IF NOT EXISTS FOR (n:`%s`) REQUIRE n.%s IS UNIQUE;";
            default:
                throw new IllegalArgumentException("Unsupported version: " + version);
        }
    }

    String indexQuery(IndexData indexData) {
        final String name = indexData.getName();
        final String label = Iterables.firstOrNull(indexData.getLabelsOrTypes());

        // create an index
        final String IDX_FMT =
                "CREATE INDEX %s IF NOT EXISTS FOR (n:`%s`) ON (%s) OPTIONS { indexProvider: '%s' };";
        final String indexProvider = indexData.getIndexProvider();
        // make sure to quote all the properties of an index
        final UnaryOperator<String> propFx = p -> "n.`" + p + "`";
        final String properties =
                indexData.getProperties().stream().map(propFx).collect(joining(","));
        return String.format(IDX_FMT, name, label, properties, indexProvider);
    }

    String constraintQuery(Neo4jVersion version, IndexData indexData) {
        final String name = indexData.getName();
        final String label = Iterables.firstOrNull(indexData.getLabelsOrTypes());

        // create constraint
        final String format = createConstraintFormat(version);
        final String firstProp = Iterables.firstOrNull(indexData.getProperties());
        return String.format(format, name, label, firstProp);
    }

    String indexOrConstraintQuery(Neo4jVersion version, IndexData indexData) {
        return indexData.isUniqueness()
                ? constraintQuery(version, indexData)
                : indexQuery(indexData);
    }

    IndexStatus indexProgress(final Driver driver, String name) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(tx -> toIndexState(tx, name));
        }
    }

    IndexStatus toIndexState(Transaction tx, String name) {
        final String FMT = "show indexes yield populationPercent,state,name WHERE name = \"%s\"";
        final var result = tx.run(String.format(FMT, name));
        final var record = Iterables.firstOrNull(result.list());
        if (null == record) {
            return IndexStatus.builder().state(State.FAILED).build();
        }
        final var pct = record.get("populationPercent").asFloat();
        final var state = record.get("state").asString("");
        return IndexStatus.builder().progress(pct).state(toState(state)).build();
    }

    IndexStatus.State toState(String state) {
        if (state.equalsIgnoreCase("FAILED")) return State.FAILED;
        if (state.equalsIgnoreCase("ONLINE")) return State.ONLINE;
        if (state.equalsIgnoreCase("POPULATING")) return State.POPULATING;
        return State.OTHER;
    }

    void writeIndexes(List<IndexData> indexes) {
        final var gson = new GsonBuilder().create();
        try (final var wrt = new BufferedWriter(new FileWriter(getFilename()))) {
            for (IndexData index : indexes) {
                wrt.write(gson.toJson(index));
                wrt.newLine();
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /** Read all the index and constraints in order, of constrains first. */
    List<IndexData> readIndexes(Driver driver) {
        try (Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(
                    tx -> {
                        final var result = tx.run("show indexes;");
                        return result.list().stream()
                                .map(AbstractIndexCommand::fromRecord)
                                .collect(Collectors.toList());
                    });
        }
    }

    String dropQuery(IndexData data) {
        final var FMT = data.isUniqueness() ? "DROP CONSTRAINT %s" : "DROP INDEX %s";
        return String.format(FMT, data.getName());
    }

    void dropIndex(Driver driver, IndexData indexData) {
        // query for all the indexes
        final var query = dropQuery(indexData);
        println(query);
        try {
            writeTransaction(driver, query);
        } catch (Throwable th) {
            LOG.error("Failed to drop index: {}", query, th);
        }
    }
}
