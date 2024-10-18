package org.neo4j.tool.copy;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;
import io.reactivex.rxjava3.core.FlowableOnSubscribe;
import io.reactivex.rxjava3.functions.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.summary.ResultSummary;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static org.neo4j.tool.copy.FlowableRetryUtil.exponentialBackoff;


/**
 * Specialized version of the CypherService with SlowQuery support w/o using "securityService" since
 * there's transactions securityService to determine the current user.
 */
@Slf4j
@RequiredArgsConstructor
public class Neo4jCypherServiceImpl implements Neo4jCypherService {

  private static final SessionConfig READ =
      SessionConfig.builder().withDefaultAccessMode(AccessMode.READ).build();

  private final Driver driver;

  private final Cache<Long, AsyncSession> sessionCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofMinutes(10))
          .evictionListener(
              (RemovalListener<Long, AsyncSession>)
                  (id, s, removalCause) -> {
                    if (removalCause == RemovalCause.EXPIRED && s != null) {
                      s.closeAsync();
                    }
                  })
          .build();

  @Override
  public Flowable<Record> readAsync(FlowableRetryUtil.Config config, CypherQueryContainer container) {
    final FlowableOnSubscribe<Record> flowable =
        emitter -> {
          // use the thread ID for synchronization
          final long id = Thread.currentThread().getId();
          final var session = acquireSession(id);
          try {
            final var work = buildWork(container, emitter);
            session.readTransactionAsync(work, transactionConfig(config));
          } catch (Throwable th) {
            emitter.onError(th);
            session.closeAsync();
          }
        };
    final Function<Flowable<? extends Throwable>, Flowable<?>> retryFx = exponentialBackoff(config);
    return Flowable.create(flowable, BackpressureStrategy.BUFFER).retryWhen(retryFx);
  }

  AsyncTransactionWork<CompletionStage<ResultSummary>> buildWork(
      final CypherQueryContainer container, final FlowableEmitter<Record> emitter) {
    return tx ->
        tx.runAsync(container.getQuery(), container.getParameters())
            .thenCompose(
                c ->
                    c.forEachAsync(
                        x -> {
                          log.trace("{}", x);
                          emitter.onNext(x);
                        }))
            .whenComplete(
                (summary, th) -> {
                  if (null != th) {
                    emitter.onError(th);
                  } else {
                    emitter.onComplete();
                  }
                });
  }

  /** For this thread acquire a session. */
  AsyncSession acquireSession(long id) {
    // safer considering the removal listener and `asMap` is mostly a reference
    final AsyncSession session = sessionCache.asMap().remove(id);
    return session != null ? session : driver.asyncSession(READ);
  }

  static TransactionConfig transactionConfig(FlowableRetryUtil.Config config) {
    final Map<String, Object> meta = new HashMap<>(config.getItems());
    meta.putIfAbsent("username", config.getUsername());
    meta.putIfAbsent("txId", config.getTransactionId());
    return TransactionConfig.builder().withTimeout(config.getTimeout()).withMetadata(meta).build();
  }
}
