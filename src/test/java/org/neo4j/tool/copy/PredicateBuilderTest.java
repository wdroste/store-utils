/*
 * Copyright 2002 Brinqa, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    @Test
    public void testLabelsScript2() {
        try (PredicateBuilder builder = new PredicateBuilder()) {
            var p = builder.newInstance("!node.labels.contains('x')");
            Assert.assertFalse(p.test(new NodeObject(List.of("x", "y", "z"), Map.of())));
            Assert.assertTrue(p.test(new NodeObject(List.of("y", "z"), Map.of())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    final String TEST_PROPERTIES =
            "(node.labels.contains('x') && node.containsProperty('timestamp')) ? (node.timestamp > 10) : true";

    @Test
    public void testFilterProperties() {
        try (PredicateBuilder builder = new PredicateBuilder()) {
            var p = builder.newInstance(TEST_PROPERTIES);
            Assert.assertFalse(
                    p.test(new NodeObject(List.of("x", "y", "z"), Map.of("timestamp", 5))));
            Assert.assertTrue(p.test(new NodeObject(List.of("x", "y", "z"), Map.of())));
            Assert.assertTrue(p.test(new NodeObject(List.of("y", "z"), Map.of())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
