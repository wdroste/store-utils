package org.neo4j.tool.dto;

import java.util.Collections;

import com.google.common.collect.ImmutableSet;

import org.neo4j.graphdb.Label;

import lombok.Value;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.neo4j.tool.StoreCopyUtil.labelInSet;

public class StoreCopyUtilTest {

    @Value(staticConstructor = "of")
    private static class TestLabel implements Label {

        String name;

        @Override
        public String name() {
            return name;
        }
    }

    @Test
    public void testLabelsInList() {
        // simple always false
        Assert.assertFalse(labelInSet(singleton(TestLabel.of("ABC")), null));
        Assert.assertFalse(labelInSet(singleton(TestLabel.of("ABC")), Collections.emptySet()));
        // test true
        Assert.assertTrue(labelInSet(singleton(TestLabel.of("ABC")), singleton("ABC")));
        // test false
        Assert.assertFalse(labelInSet(singleton(TestLabel.of("ABC")), singleton("AB")));

        Assert.assertFalse(labelInSet(singleton(TestLabel.of("ABC")), ImmutableSet.of("AB", "AA")));
        Assert.assertFalse(labelInSet(ImmutableSet.of(TestLabel.of("ABC"), TestLabel.of("XYZ")), ImmutableSet.of("AB", "AA")));

        Assert.assertTrue(labelInSet(ImmutableSet.of(TestLabel.of("ABC"), TestLabel.of("AA")), ImmutableSet.of("AB", "AA")));
    }
}
