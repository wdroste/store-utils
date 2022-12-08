package org.neo4j.tool.dto;

import com.google.common.collect.ImmutableSet;
import com.google.gson.GsonBuilder;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class SerializationTest {

    @Test
    public void testSerialization() {
        val cfg = new StoreCopyConfiguration();
        cfg.setIgnoreLabels(ImmutableSet.of("IgnoreLabel1", "IgnoreLabel2"));
        cfg.setDeleteNodesWithLabels(ImmutableSet.of("DeleteLabel1", "DeleteLabel2"));

        val gson = new GsonBuilder().setPrettyPrinting().create();
        val json = gson.toJson(cfg);
        System.out.println(json);

        // round trip
        val actual = gson.fromJson(json, StoreCopyConfiguration.class);
        Assert.assertEquals(cfg, actual);
    }
}
