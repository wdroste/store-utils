package org.neo4j.tool.util;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class PredicateHelper {

    /**
     * Check if the labels match.
     *
     * @param deleteNodesWithLabels nodes with labels that should be deleted.
     * @return true if provided labels are <i>not</i> in the set
     */
    public static Predicate<List<String>> buildLabelInverseMatcher(
            Set<String> deleteNodesWithLabels) {
        return labelNames ->
                deleteNodesWithLabels.isEmpty()
                        || labelNames.stream()
                                .filter(deleteNodesWithLabels::contains)
                                .findFirst()
                                .isEmpty();
    }
}
