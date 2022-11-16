package org.neo4j.tool;


import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

/**
 * Determine the version of Neo4j
 */
public class VersionQuery {

    enum Neo4jVersion {
        v4_2,
        v4_3,
        v4_4,
        v3_5
    }

    private static final String QUERY =
        "call dbms.components() yield versions unwind versions as version return version;";

    public static Neo4jVersion determineVersion(Driver driver) {
        try (Session s = driver.session()) {
            return s.readTransaction(
                tx ->
                    tx.run(QUERY).list().stream()
                        .findFirst()
                        .map(r -> r.get(0).asString())
                        .map(VersionQuery::toVersion)
                        .orElse(null));
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Neo4jVersion toVersion(String ver) {
        if (ver.startsWith("4.4")) {
            return Neo4jVersion.v4_4;
        }
        if (ver.startsWith("4.3")) {
            return Neo4jVersion.v4_3;
        }
        if (ver.startsWith("4.2")) {
            return Neo4jVersion.v4_2;
        }
        if (ver.startsWith("3.5")) {
            return Neo4jVersion.v3_5;
        }
        throw new IllegalArgumentException("Unknown version " + ver);
    }
}
