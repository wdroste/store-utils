package org.neo4j.tool.copy;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.tool.util.PredicateHelper;

public class PredicateHelperTest {

    @Test
    public void simpleTest() {
        Predicate<List<String>> p = PredicateHelper.buildLabelInverseMatcher(Set.of("x", "y"));
        Assert.assertTrue(p.test(List.of("a")));
        Assert.assertFalse(p.test(List.of("x")));
        Assert.assertFalse(p.test(List.of("y")));
        Assert.assertFalse(p.test(List.of("x", "y")));
    }

    @Test
    public void emptyTest() {
        Predicate<List<String>> p = PredicateHelper.buildLabelInverseMatcher(Set.of());
        Assert.assertTrue(p.test(List.of("a")));
        Assert.assertTrue(p.test(List.of("x")));
    }
}
