package org.neo4j.tool.copy;

import io.reactivex.rxjava3.functions.Function;
import org.neo4j.cypherdsl.core.Cypher;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Neo4jHelper {
    // === Paginate over the label provided
    public static final String OBJECT_NODE = "n";
    public static final String LIMIT_PARAM = "limit";
    public static final String LAST_KEY_PARAM = "lastKey";

    // build the query to get all the records (sources)
    public static String buildPaginateIdsQuery(String nodeLabel) {
        final var node = Cypher.node(nodeLabel).named(OBJECT_NODE);
        final var idProp = node.property("id");

        final var match = Cypher.match(node);
        return match
                .where(idProp.gt(Cypher.parameter(LAST_KEY_PARAM)))
                .returning(idProp)
                .orderBy(idProp)
                .limit(Cypher.parameter(LIMIT_PARAM))
                .build()
                .getCypher();
    }

    public static final String IDS_PARAM = "ids";
    public static final String REL_ATTRS = "_relAttrs";
    public static final String ASSOCIATION_NAME = "associationName";
    // NOTE: This needs some work. We could be more prescriptive by using
    // ONE-to-ONE relationships with properly modeled relationship types.
    // Unfortunately, the types we use (e.g. HAS, THAT) are not crisply
    // modeled relationship types.
    private static final String QUERY =
            "MATCH (n:`%s`) WHERE n.id IN $ids"
                    + " RETURN n{%s, "
                    + REL_ATTRS
                    + ": [(n)-[r]->(t:DataSet) | r{."
                    + ASSOCIATION_NAME
                    + ", id: t.id}]}";

    // build the query to get all the records (sources)
    public static String buildAcquisitionQuery(String nodeLabel) {
        return buildAcquisitionQuery(nodeLabel, List.of());
    }

    public static String buildAcquisitionQuery(String nodeLabel, Collection<String> attributesToGet) {
        final var attrs =
                attributesToGet.isEmpty()
                        ? ".*"
                        : attributesToGet.stream()
                        .map(attrName -> format(".%s", attrName))
                        .collect(Collectors.joining(","));
        return format(QUERY, nodeLabel, attrs);
    }

    public static Function<List<Long>, CypherQueryContainer> buildAcquisitionContainerCreator(
            final String query) {
        final var bld = CypherQueryContainer.builder().query(query);
        return ids -> bld.clearParameters().parameter(IDS_PARAM, ids).build();
    }

    public static String buildAcquisitionQueryV2(String name) {
        final var fmt = "MATCH (%s:`%s`) WHERE n.id IN $ids return %s";
        return String.format(fmt, OBJECT_NODE, name, OBJECT_NODE);
    }

    public static String buildAcquisitionQueryWithLabels(String dataModelName, String labelsAttributeName) {
        final var fmt = "MATCH (n:`%s`) WHERE n.id IN $ids " +
                "RETURN %s{.*, %s: [(n)-[r:LABELS]->(l:Label) | l.name]}";
        return String.format(fmt, dataModelName, OBJECT_NODE, labelsAttributeName);
    }
}
