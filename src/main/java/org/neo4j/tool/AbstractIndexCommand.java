package org.neo4j.tool;

import static java.util.stream.Collectors.joining;
import static org.neo4j.tool.Print.println;

import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.internal.helpers.collection.Iterables;
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
                record.get(9).asString());
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

    void createIndex(final Driver driver, IndexData index) {
        final var query = indexQuery(index);
        println(query);
        try {
            writeTransaction(driver, query);
        } catch (Throwable th) {
            LOG.error("Failed to create index: {}", query, th);
        }
    }

    void createIndexWaitForCompletion(final Driver driver, final IndexData index) {
        createIndex(driver, index);

        // wait for completion
        int pct = 0;
        while (pct < 100) {
            progressPercentage(pct);
            pct = (int) indexProgress(driver, index.getName());
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

    String indexQuery(IndexData indexData) {
        final String CNT_FMT =
                "CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:`%s`) REQUIRE n.%s IS UNIQUE;";
        final String IDX_FMT =
                "CREATE INDEX %s IF NOT EXISTS FOR (n:`%s`) ON (%s) OPTIONS { indexProvider: '%s' };";

        final String name = indexData.getName();
        final String indexProvider = indexData.getIndexProvider();
        final String label = Iterables.firstOrNull(indexData.getLabelsOrTypes());
        final String firstProp = Iterables.firstOrNull(indexData.getProperties());

        final UnaryOperator<String> propFx = p -> "n.`" + p + "`";
        final String properties =
                indexData.getProperties().stream().map(propFx).collect(joining(","));

        return indexData.isUniqueness()
                ? String.format(CNT_FMT, name, label, firstProp)
                : String.format(IDX_FMT, name, label, properties, indexProvider);
    }

    float indexProgress(final Driver driver, String name) {
        final String FMT = "show indexes yield populationPercent, name WHERE name = \"%s\"";
        // query for all the indexes
        try (Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(
                    tx -> {
                        final var result = tx.run(String.format(FMT, name));
                        final var record = Iterables.firstOrNull(result.list());
                        return (null == record) ? 0 : record.get(0).asFloat();
                    });
        }
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
                                .sorted(new IndexDataComparator())
                                .collect(Collectors.toList());
                    });
        }
    }

    static class IndexDataComparator implements Comparator<IndexData> {
        private final StringListComparator stringListComparator = new StringListComparator();

        @Override
        public int compare(IndexData o1, IndexData o2) {
            // constraints first
            int cmp = Boolean.compare(o1.isUniqueness(), o2.isUniqueness());
            if (0 != cmp) {
                return cmp;
            }
            // type
            cmp = o1.getType().compareTo(o2.getType());
            if (0 != cmp) {
                return cmp;
            }
            // compare labels
            cmp = stringListComparator.compare(o1.getLabelsOrTypes(), o2.getLabelsOrTypes());
            if (0 != cmp) {
                return cmp;
            }
            // name
            return o1.getName().compareTo(o2.getName());
        }
    }

    /** Just sort based on number of items in list first (more complex) to least complex. */
    static class StringListComparator implements Comparator<List<String>> {

        @Override
        public int compare(List<String> l1, List<String> l2) {
            int cmp = Integer.compare(l1.size(), l2.size());
            if (0 != cmp) {
                return cmp;
            }

            final List<String> s1 = l1.stream().sorted().collect(Collectors.toList());
            final List<String> s2 = l2.stream().sorted().collect(Collectors.toList());
            for (int i = 0; i < s1.size(); i++) {
                final String sc1 = s1.get(i);
                cmp = sc1.compareTo(s2.get(i));
                if (0 != cmp) {
                    return cmp;
                }
            }
            return 0;
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
