package org.neo4j.tool.copy;

import com.brinqa.composite.dto.ProviderConfig;
import com.brinqa.composite.impl.WorkContext;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.neo4j.driver.Record;
import org.neo4j.tool.copy.FlowableRetryUtil.Config;

import java.util.List;

import static com.brinqa.provider.neo4j.Neo4jHelper.buildAcquisitionContainerCreator;

public class Neo4jDataProvider {

  private final Config configuration;
  private final ProviderConfig providerConfig;
  private final Neo4jCypherService cypherService;
  private final Neo4jKeyPaginatorFactory paginatorFactory;

  public Neo4jDataProvider(final WorkContext workContext, final ProviderConfig providerConfig) {
    this.cypherService = new Neo4jCypherServiceImpl(workContext.getDriver());
    this.paginatorFactory = new Neo4jKeyPaginatorFactory(cypherService);
    this.providerConfig = providerConfig;
    this.configuration =
        Config.builder()
            .exponentialBackoff(
                Config.ExponentialBackoff.builder().count(providerConfig.getMaxRetries()).build())
            .username(workContext.getUsername())
            .transactionId(workContext.getTxId())
            .timeout(providerConfig.getTimeout())
            .build();
  }

  public Flowable<Record> read(final String nodeLabel, final String acquisitionQuery) {
    final int limit = providerConfig.getLimit();
    final int batchSize = providerConfig.getBatchSize();
    final int maxConcurrentRequests = providerConfig.getParallel();

    final var acquisitionCntFx = buildAcquisitionContainerCreator(acquisitionQuery);
    final var paginator = paginatorFactory.buildPagination(configuration, nodeLabel, limit);
    final Function<List<Long>, Flowable<Record>> fx =
        ids -> {
          // check if there's anything to do
          if (ids.isEmpty()) {
            return Flowable.empty();
          }
          int batchCount = ids.size() / batchSize + (ids.size() % batchSize == 0 ? 0 : 1);
          return Flowable.range(0, batchCount)
              .map(
                  i -> {
                    int toIndex = Math.min(ids.size(), batchSize * (i + 1));
                    return ids.subList(i * batchSize, toIndex);
                  })
              .flatMap(
                  batchIds -> {
                    final var cnt = acquisitionCntFx.apply(batchIds);
                    return cypherService.readAsync(configuration, cnt);
                  },
                  maxConcurrentRequests);
        };

    return Flowable.defer(() -> paginator.execute().flatMap(fx))
        .repeatUntil(paginator::isFinish)
        .observeOn(Schedulers.io());
  }
}
