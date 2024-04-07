package org.neo4j.tool.copy;

import com.brinqa.platform.compute.ComputeCypherService;
import com.brinqa.platform.compute.ComputeCypherService.Config;
import com.brinqa.platform.v3.analytics.warehouse.service.DataModelDataProvider.Data;
import com.brinqa.platform.v3.analytics.warehouse.service.RelationshipsDataProvider;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Function;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Record;
import org.reactivestreams.Publisher;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.brinqa.platform.v3.analytics.warehouse.util.Neo4jHelper.buildAcquisitionContainerCreator;

/**
 * The purpose of this class is to use key pagination to go through every DataSet label in the
 * system and determine the incoming and outgoing relationships.
 *
 * <p>NOTE: Idk yet if it's better to use the child labels or just use DataSet label. The simple
 * solution is to use the DataSet label. There's some advantages to using each label as one can be
 * more prescriptive.
 */
@Slf4j
@AllArgsConstructor
public class Neo4jRelationshipsDataProvider {
  private static final String ACQUISITION_QUERY =
      String.join(
          " ",
          new String[] {
            "MATCH (s:%s)-[r]->(e:%s)",
            "WHERE s.id IN $ids",
            "RETURN",
            "s.id as sid,",
            "e.id as eid,",
            "r",
          });

  private final ComputeCypherService cypherService;
  private final ConfigurationHelper configurationHelper;
  private final Neo4jKeyPaginatorFactory paginatorFactory;

  public Flowable<Data> read() {
    // build all the configuration
    final var limit = configurationHelper.paginationLimit();
    final var batchSize = configurationHelper.acquisitionBatchSize();
    final var configuration = configurationHelper.buildConfiguration();
    final var acquisitionFx = buildAcquisitionFx(configuration);
    final var paginator = paginatorFactory.buildPagination(configuration, "DataSet", limit);
    return Flowable.defer(
            () ->
                paginator
                    .execute()
                    .flattenStreamAsFlowable(LongArrayList::stream)
                    .buffer(batchSize, LongArrayList::new)
                    .flatMap(acquisitionFx)
                    .map(this::convert))
        .repeatUntil(paginator::isFinish);
  }

  Function<List<Long>, Publisher<? extends Record>> buildAcquisitionFx(final Config config) {
    final var acquisitionCntFx = buildAcquisitionContainerCreator(ACQUISITION_QUERY);
    return list -> {
      // check if there's anything to do
      if (list.isEmpty()) {
        return Flowable.empty();
      }
      // query for the batch
      final var cnt = acquisitionCntFx.apply(list);
      return cypherService.readAsync(config, cnt);
    };
  }

  /** Basically convert from Neo4j object to a Brinqa object. */
  Data convert(final @NonNull Record record) {
    // index is much faster when working w/ millions
    return Data.builder()
        .startNode(record.get(0).asLong())
        .endNode(record.get(1).asLong())
        .associationName(record.get(2).asString(null))
        .startNodeDataModelId(record.get(3).asLong())
        .endNodeDataModelId(record.get(4).asLong())
        .build();
  }
}
