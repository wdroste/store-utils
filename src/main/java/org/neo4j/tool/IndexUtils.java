package org.neo4j.tool;

import lombok.val;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.tool.dto.IndexData;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class IndexUtils {

    public static String buildIndex(IndexData indexData) {
        val label = Iterables.first(indexData.getLabelsOrTypes());
        val properties = String.join(",", indexData.getProperties());
        // create an index
        return String.format(":`%s`(%s)", label, properties);
    }

    public static String indexOrConstraintQuery(IndexData indexData) {
        val procedure = indexData.isUniqueness()
                ? "createUniquePropertyConstraint"
                : "createIndex";
        val indexProvider = isNotBlank(indexData.getIndexProvider())
                ? indexData.getIndexProvider()
                : "native-btree-1.0";
        val index = buildIndex(indexData);
        // create an index
        val FMT = "CALL db.%s(\"%s\",\"%s\")";
        // make sure to quote all the properties of an index
        return String.format(FMT, procedure, index, indexProvider);
    }
}
