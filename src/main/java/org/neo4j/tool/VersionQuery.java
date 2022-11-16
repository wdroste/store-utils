package org.neo4j.tool;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

/** Determine the version of Neo4j */
public class VersionQuery {

    enum Neo4jVersion {
        v4_2,
        v4_3,
        v4_4
    }

    private static final String QUERY =
            "call dbms.components() yield versions unwind versions as version return version;";

    public static Neo4jVersion determineVersion(Driver driver) {
        try (Session s = driver.session()) {
            return s.readTransaction(VersionQuery::toVersion);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Neo4jVersion toVersion(Transaction tx) {
        return tx.run(QUERY).list().stream()
                .findFirst()
                .map(r -> r.get(0).asString())
                .map(VersionQuery::toVersion)
                .orElse(null);
    }

    static Neo4jVersion toVersion(String ver) {
        if (ver.startsWith("4.4"))
            return Neo4jVersion.v4_4;
        if (ver.startsWith("4.3"))
            return Neo4jVersion.v4_3;
        if (ver.startsWith("4.2"))
            return Neo4jVersion.v4_2;
        throw new IllegalArgumentException("Unknown version " + ver);
    }
}
