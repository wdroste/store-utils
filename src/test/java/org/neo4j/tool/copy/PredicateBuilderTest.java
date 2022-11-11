package org.neo4j.tool.copy;

import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class PredicateBuilderTest {

    //
    @Test
    public void testNullScript() {
        try (PredicateBuilder builder = new PredicateBuilder()) {
            var p = builder.newInstance(null);
            Assert.assertTrue(p.test(null));
            Assert.assertTrue(p.test(new NodeObject(List.of("x"), Map.of())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBlankScript() {
        try (PredicateBuilder builder = new PredicateBuilder()) {
            var p = builder.newInstance(" ");
            Assert.assertTrue(p.test(null));
            Assert.assertTrue(p.test(new NodeObject(List.of("x"), Map.of())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTrueScript() {
        try (PredicateBuilder builder = new PredicateBuilder()) {
            var p = builder.newInstance("true");
            Assert.assertTrue(p.test(null));
            Assert.assertTrue(p.test(new NodeObject(List.of("x"), Map.of())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLabelsScript() {
        try (PredicateBuilder builder = new PredicateBuilder()) {
            var p = builder.newInstance("node.labels.contains('x')");
            Assert.assertTrue(p.test(new NodeObject(List.of("x"), Map.of())));
            Assert.assertFalse(p.test(new NodeObject(List.of("y"), Map.of())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
