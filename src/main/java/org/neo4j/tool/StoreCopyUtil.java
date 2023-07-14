package org.neo4j.tool;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.internal.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.internal.DirectRecordAccess;
import org.neo4j.unsafe.batchinsert.internal.DirectRecordAccessSet;
import org.neo4j.unsafe.batchinsert.internal.FileSystemClosingBatchInserter;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Static methods from StoreUtil.
 */
public class StoreCopyUtil {

    private static final Label[] NO_LABELS = new Label[0];

    public static boolean labelInSet(Iterable<Label> nodeLabels, Set<String> labelSet) {
        if (labelSet == null || labelSet.isEmpty()) {
            return false;
        }
        for (Label nodeLabel : nodeLabels) {
            if (labelSet.contains(nodeLabel.name())) {
                return true;
            }
        }
        return false;
    }

    public static Label[] toArray(Iterable<Label> nodeLabels) {
        final Collection<Label> labels = Iterables.asList(nodeLabels);
        if (labels.isEmpty()) {
            return NO_LABELS;
        }
        return labels.toArray(new Label[0]);
    }

    public interface Flusher {

        void flush();
    }

    public static Flusher buildFlusher(BatchInserter db) {
        try {
            Field delegate = FileSystemClosingBatchInserter.class.getDeclaredField("delegate");
            delegate.setAccessible(true);
            db = (BatchInserter) delegate.get(db);
            Field field = BatchInserterImpl.class.getDeclaredField("recordAccess");
            field.setAccessible(true);
            final DirectRecordAccessSet recordAccessSet = (DirectRecordAccessSet) field.get(db);
            final Field cacheField = DirectRecordAccess.class.getDeclaredField("batch");
            cacheField.setAccessible(true);
            return new Flusher() {
                @Override
                public void flush() {
                    try {
                        ((Map) cacheField.get(recordAccessSet.getNodeRecords())).clear();
                        ((Map) cacheField.get(recordAccessSet.getRelRecords())).clear();
                        ((Map) cacheField.get(recordAccessSet.getPropertyRecords())).clear();
                    }
                    catch (IllegalAccessException e) {
                        throw new RuntimeException("Error clearing cache " + cacheField, e);
                    }
                }
            };
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Error accessing cache field ", e);
        }
    }
}
