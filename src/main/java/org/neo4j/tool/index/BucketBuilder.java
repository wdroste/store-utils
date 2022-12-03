package org.neo4j.tool.index;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.tool.dto.Bucket;
import org.neo4j.tool.dto.Bucket.Size;
import org.neo4j.tool.dto.IndexBatch;
import org.neo4j.tool.dto.IndexData;

public class BucketBuilder {

    public static List<Bucket> build(List<Pair<IndexData, Long>> pairs) {
        final Map<Size, ArrayList<IndexData>> map = new HashMap<>();
        for (Pair<IndexData, Long> pair : pairs) {
            final var size = toSize(pair.getValue());
            final var l = map.computeIfAbsent(size, na -> new ArrayList<>());
            l.add(pair.getKey());
        }

        // break up into buckets
        return map.entrySet().stream()
                .flatMap(e -> toBuckets(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    static Stream<Bucket> toBuckets(Bucket.Size size, List<IndexData> values) {
        return Lists.partition(values, 100).stream().map(b -> toBucket(size, b));
    }

    static Bucket toBucket(Bucket.Size size, List<IndexData> values) {
        final var batch = IndexBatch.builder().indexes(values).build();
        return Bucket.builder().size(size).batch(batch).build();
    }

    static Size toSize(long total) {
        if (total < 1000) {
            return Size.SMALL;
        }
        if (total < 100_000) {
            return Size.MEDIUM;
        }
        return Size.LARGE;
    }
}
