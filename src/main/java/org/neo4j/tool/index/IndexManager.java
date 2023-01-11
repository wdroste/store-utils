/*
 * Copyright 2002 Brinqa, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.tool.index;

import static java.util.stream.Collectors.joining;
import static org.neo4j.tool.util.Print.println;
import static org.neo4j.tool.util.Print.progressPercentage;

import com.google.gson.GsonBuilder;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.neo4j.driver.v1.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.tool.VersionQuery;
import org.neo4j.tool.VersionQuery.Neo4jVersion;
import org.neo4j.tool.dto.*;
import org.neo4j.tool.dto.IndexStatus.State;
import org.parboiled.common.ImmutableList;

@Slf4j
@AllArgsConstructor
public class IndexManager {

    private final Driver driver;

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
        return (null == value || value.isNull())
                ? ImmutableList.of()
                : value.asList(Value::asString);
    }

    public List<IndexData> readIndexesFromFile(File f) {
        val ret = new ArrayList<IndexData>();
        val gson = new GsonBuilder().create();
        try (val rdr = new BufferedReader(new FileReader(f))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                val index = gson.fromJson(line, IndexData.class);
                ret.add(index);
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return ret;
    }

    void createIndex(Neo4jVersion version, IndexData index) {
        val query = indexOrConstraintQuery(version, index);
        println(query);
        try {
            writeTransaction(query);
        } catch (Throwable th) {
            log.error("Failed to create index: {}", query, th);
            throw new RuntimeException(th);
        }
        if (!validIndex(index)) {
            println("Failed to create index: %s", index.getName());
        }
    }

    boolean validIndex(final IndexData index) {
        // insure index creation started
        for (int i = 0; i < 10; i++) {
            val status = indexProgress(index);
            if (null != status && status.getState().isOk()) {
                return true;
            }
            simpleWait(10);
        }
        return false;
    }

    private void simpleWait(long milliseconds) {
        try {
            TimeUnit.MILLISECONDS.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void monitorCreation(final IndexData index) {
        println("Monitoring: %s", index.getName());
        // wait for completion
        int pct = 0;
        while (pct < 100) {
            progressPercentage(pct);
            val status = indexProgress(index);
            if (status.getState().isFailed()) {
                println("%nFailed to create index: %s", index.getName());
                return;
            }
            pct = (int) status.getProgress();
            progressPercentage(pct);
            simpleWait(100);
        }

        // index creation is finished
        if (!index.isUniqueness()) {
            return;
        }

        // loop waiting for a bit for it to be created fail after 10 secs
        for (int i = 0; i < 100; i++) {
            val status = constraintCheck(index.getName());
            if (status.isOnline()) {
                return;
            }
            simpleWait(100);
        }
        val ERROR_FMT = "Constraint '%s' failed to come online, please create manually.";
        throw new IllegalStateException(String.format(ERROR_FMT, index.getName()));
    }

    void writeTransaction(final String query) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            val resultSummary = session.writeTransaction(tx -> tx.run(query).consume());
            if (log.isDebugEnabled()) {
                log.debug(resultSummary.toString());
            }
        }
    }

    <T> T readTransaction(TransactionWork<T> work) {
        // query for all the indexes
        try (final Session session = driver.session(AccessMode.READ)) {
            assert session != null;
            return session.readTransaction(work);
        }
    }

    String indexQuery(IndexData indexData) {
        final String label = Iterables.firstOrNull(indexData.getLabelsOrTypes());
        // create an index
        final String IDX_FMT = "CREATE INDEX ON :`%s`(%s);";
        // make sure to quote all the properties of an index
        return String.format(IDX_FMT, label, propertiesArgument(indexData));
    }

    String propertiesArgument(IndexData indexData) {
        final UnaryOperator<String> propFx = p -> '`' + p + '`';
        return indexData.getProperties().stream().map(propFx).collect(joining(","));
    }

    String constraintQuery(IndexData indexData) {
        final String label = Iterables.firstOrNull(indexData.getLabelsOrTypes());
        final String format = "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE;";
        final String firstProp = Iterables.firstOrNull(indexData.getProperties());
        return String.format(format, label, firstProp);
    }

    public String indexOrConstraintQuery(Neo4jVersion version, IndexData indexData) {
        return indexData.isUniqueness() ? constraintQuery(indexData) : indexQuery(indexData);
    }

    IndexStatus indexProgress(IndexData indexData) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(tx -> toIndexState(tx, indexData));
        }
    }

    ConstraintStatus constraintCheck(String name) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(tx -> toConstraintStatus(tx, name));
        }
    }

    ConstraintStatus toConstraintStatus(Transaction tx, String name) {
        final String FMT = "show constraints yield name WHERE name = \"%s\"";
        val result = tx.run(String.format(FMT, name));
        val record = Iterables.firstOrNull(result.list());
        return ConstraintStatus.of(null != record);
    }

    IndexStatus toIndexState(Transaction tx, IndexData index) {
        val fmt =
                "call db.indexes() yield description, state, progress WHERE description contains \":%s(%s)\" return state, progress";
        val props = String.join(",", index.getProperties());
        val label = Iterables.firstOrNull(index.getLabelsOrTypes());
        val result = tx.run(String.format(fmt, label, props));
        val record = Iterables.firstOrNull(result.list());
        if (null == record) {
            return IndexStatus.builder().state(State.FAILED).build();
        }
        val pct = record.get("progress").asFloat(0);
        val state = record.get("state").asString("");
        return IndexStatus.builder().progress(pct).state(toState(state)).build();
    }

    State toState(String state) {
        if (state.equalsIgnoreCase("FAILED")) {
            return State.FAILED;
        }
        if (state.equalsIgnoreCase("ONLINE")) {
            return State.ONLINE;
        }
        if (state.equalsIgnoreCase("POPULATING")) {
            return State.POPULATING;
        }
        return State.OTHER;
    }

    public void writeIndexes(List<IndexData> indexes, File file) {
        val gson = new GsonBuilder().create();
        try (val wrt = new BufferedWriter(new FileWriter(file))) {
            for (IndexData index : indexes) {
                wrt.write(gson.toJson(index));
                wrt.newLine();
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /** Read all the index and constraints in order, of constrains first. */
    public List<IndexData> readDBIndexes() {
        try (Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(
                    tx -> {
                        val result = tx.run("show indexes;");
                        return result.list().stream()
                                .map(IndexManager::fromRecord)
                                .collect(Collectors.toList());
                    });
        }
    }

    String dropQuery(final IndexData data) {
        val FMT = (data.isUniqueness() ? "DROP CONSTRAINT %s" : "DROP INDEX %s") + " IF EXISTS;";
        return String.format(FMT, data.getName());
    }

    public void dropIndex(final IndexData indexData) {
        // query for all the indexes
        val query = dropQuery(indexData);
        println(query);
        try {
            writeTransaction(query);

        } catch (Throwable th) {
            log.error("Failed to drop index: {}", query, th);
        }
    }

    public Set<String> readIndexNames() {
        return readDBIndexes().stream().map(IndexData::getName).collect(Collectors.toSet());
    }

    public void createAndMonitor(Neo4jVersion version, IndexData index, boolean recreate) {
        if (recreate) {
            dropIndex(index);
        }
        createIndex(version, index);
        monitorCreation(index);
    }

    public Neo4jVersion determineVersion() {
        return VersionQuery.determineVersion(driver);
    }

    public long labelSize(String labelName) {
        final String FMT = "MATCH (n:`%s`) return count(n) as count";
        return this.readTransaction(
                tx -> {
                    val result = tx.run(String.format(FMT, labelName));
                    return Optional.ofNullable(Iterables.firstOrNull(result.list()))
                            .map(r -> r.get(0).asLong(0L))
                            .orElse(0L);
                });
    }

    /** Create all the indexes in one transaction for the bucket. */
    public void create(final Neo4jVersion version, final Bucket bucket) {
        for (final IndexBatch batch : bucket.getBatches()) {
            // send all the commands
            try (final Session s = driver.session()) {
                final Transaction tx = s.beginTransaction();
                for (IndexData index : batch.getIndexes()) {
                    val q = indexOrConstraintQuery(version, index);
                    println(q);
                    tx.run(q);
                }
                tx.success();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // monitor the indexes
            batch.getIndexes().forEach(this::monitorCreation);
        }
    }
}
