package org.neo4j.tool;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;

/**
 * @author mh
 * @since 21.12.11
 */
public class StoreUpdate {
    public static Map<String, String> config() {
        return MapUtil.map(
                        "neostore.nodestore.db.mapped_memory", "100M",
                        "neostore.relationshipstore.db.mapped_memory", "500M",
                        "neostore.propertystore.db.mapped_memory", "300M",
                        "neostore.propertystore.db.strings.mapped_memory", "1G",
                        "neostore.propertystore.db.arrays.mapped_memory", "300M",
                        "neostore.propertystore.db.index.keys.mapped_memory", "100M",
                        "neostore.propertystore.db.index.mapped_memory", "100M",
                        "allow_store_upgrade", "true",
                        "cache_type", "weak")
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, x -> x.getValue().toString()));
    }

    public static void main(String[] args) {
        GraphDatabaseService db = null;
        try {
            db =
                    new GraphDatabaseFactory()
                            .newEmbeddedDatabaseBuilder(new File("target/data"))
                            .setConfig(config())
                            .newGraphDatabase();
        } finally {
            if (db != null) db.shutdown();
        }
    }
}
