package org.neo4j.tool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.internal.helpers.collection.Iterables;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static java.util.stream.Collectors.joining;

/**
 * Takes a dump file and creates each of the constraints and indexes from that file in a controlled
 * manner. In particular it waits until an index is online before moving to the next as adding to
 * many indexes at any onetime will result in either an OOME or a corrupted index that will need to
 * be refreshed again.
 */
@Command(
    name = "loadIndex",
    version = "loadIndex 1.0",
    description = "Creates indexes and constraints based on the file provided.")
public class LoadIndex extends AbstractIndexCommand {

    private static final Logger LOG = LoggerFactory.getLogger(LoadIndex.class);

    @Option(
        required = true,
        names = {"-f", "--filename"},
        description = "Name of the file to load all the indexes.",
        defaultValue = "dump.json")
    protected String filename;

    @Option(
        names = {"-r", "--recreate"},
        description = "Recreate each of the indexes in the file.")
    protected boolean recreate;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new LoadIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final Driver driver) {
        final var indexes = readIndexes();
        for (int i = 0; i < indexes.size(); i++) {
            final var indexData = indexes.get(i);

            // filter TOKEN
            if (indexData.getLabelsOrTypes().isEmpty()) {
                continue;
            }

            // create the index
            indexCreate(driver, indexData);

            // create a progress bar per index, wait until the index is complete
            try (ProgressBar pb = newIndexProgressBar(indexData.getName())) {
                long pct = 0;
                while (pct < 100) {
                    pct = (long) indexProgress(driver, indexData.getName());
                    pb.stepTo(pct);
                }
            }
        }
    }

    ProgressBar newIndexProgressBar(String taskName) {
        final ProgressBarBuilder bld = new ProgressBarBuilder();
        bld.setInitialMax(100L);
        bld.setStyle(ProgressBarStyle.ASCII);
        bld.setTaskName("Creating " + taskName);
        return bld.build();
    }

    List<IndexData> readIndexes() {
        final var ret = new ArrayList<IndexData>();
        final var gson = new GsonBuilder().create();
        try (final var rdr = new BufferedReader(new FileReader(this.filename))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                final var index = gson.fromJson(line, IndexData.class);
                ret.add(index);
            }
        }
        catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return ret;
    }

    void indexCreate(final Driver driver, IndexData index) {
        // query for all the indexes
        try (Session session = driver.session()) {
            assert session != null;
            final var query = indexQuery(index);
            ResultSummary resultSummary = session.writeTransaction(tx -> tx.run(query).consume());
            if (LOG.isDebugEnabled()) {
                LOG.debug(resultSummary.toString());
            }
        }
    }

    String indexQuery(IndexData indexData) {
        final String CNT_FMT = "CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:`%s`) REQUIRE n.%s IS UNIQUE;";
        final String IDX_FMT = "CREATE INDEX %s IF NOT EXISTS FOR (n:`%s`) ON (%s);";

        final String name = indexData.getName();
        final String label = Iterables.firstOrNull(indexData.getLabelsOrTypes());
        final String firstProp = Iterables.firstOrNull(indexData.getProperties());

        final String properties = indexData.getProperties().stream()
            .map(p -> "n.`" + p + "`")
            .collect(joining(","));

        final String query = indexData.isUniqueness()
                             ? String.format(CNT_FMT, name, label, firstProp)
                             : String.format(IDX_FMT, name, label, properties);
        LOG.debug(query);
        return query;
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
}
