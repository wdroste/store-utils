package org.neo4j.tool.dto;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public class IndexRebuildIntegrationTests {

    private static final DockerImageName IMAGE = DockerImageName.parse("neo4j:3.5");

    @Container
    private static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(IMAGE)
            .withoutAuthentication(); // Disable password

    @Test
    void testSomethingUsingBolt() {
        // Retrieve the Bolt URL from the container
        String boltUrl = neo4jContainer.getBoltUrl();
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none());
             Session session = driver.session()) {
            long one = session.run("RETURN 1", emptyMap()).next().get(0).asLong();
            assertEquals(1L, one);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
