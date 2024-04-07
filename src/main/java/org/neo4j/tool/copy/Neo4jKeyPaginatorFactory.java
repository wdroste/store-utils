package org.neo4j.tool.copy;

import com.google.common.primitives.Longs;
import io.reactivex.rxjava3.core.Single;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


@RequiredArgsConstructor
public class Neo4jKeyPaginatorFactory {
  private final ComputeCypherService cypherService;

  public interface Paginator {
    boolean isFinish();

    Single<LongArrayList> execute();
  }

  public Paginator buildPagination(final Config config, String nodeLabel, final int limit) {
    // fields
    final var count = new AtomicInteger();
    final var idStorage = new LongArrayList(limit);
    final var lastRecordId = new AtomicLong(-1L);

    // const query
    final var paginationQuery = buildPaginateIdsQuery(nodeLabel);
    return new Paginator() {
      @Override
      public boolean isFinish() {
        return limit != count.get();
      }

      @Override
      public Single<LongArrayList> execute() {
        // reset count and array
        count.set(0);
        idStorage.clear();

        // determine paginationQuery container w/ last ID
        final var paginationCnt =
          CypherQueryContainer.builder()
            .query(paginationQuery)
            .parameter(Neo4jHelper.LIMIT_PARAM, limit)
            .parameter(Neo4jHelper.LAST_KEY_PARAM, lastRecordId.get())
            .build();

        // paginationQuery for the next batch of ids
        return cypherService
          .readAsync(config, paginationCnt)
          .map(r -> r.get(0).asLong())
          .toList(() -> idStorage)
          .doOnSuccess(
            list -> {
              // set the last index and count
              if (!list.isEmpty()) {
                final int lastIdx = list.size() - 1;
                final long lastId = list.getLong(lastIdx);
                // test to make sure that algo is correct
                assert Longs.max(list.toLongArray()) == lastId;
                // set the last ID
                lastRecordId.set(lastId);
                count.addAndGet(list.size());
              }
            });
      }
    };
  }
}
