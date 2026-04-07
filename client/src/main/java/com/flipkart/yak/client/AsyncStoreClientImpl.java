package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.yak.client.AsyncStoreClientUtis.IndexResponse;
import com.flipkart.yak.client.AsyncStoreClientUtis.Mutation;
import com.flipkart.yak.client.AsyncStoreClientUtis.RecurseData;
import com.flipkart.yak.client.AsyncStoreClientUtis.StorePuts;
import com.flipkart.yak.client.AsyncStoreClientUtis.Tupple;
import com.flipkart.yak.client.config.ConfigValidator;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.daemons.AsyncIndexPurgeDaemon;
import com.flipkart.yak.client.exceptions.RequestValidatorException;
import com.flipkart.yak.client.exceptions.StoreDataNotFoundException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.metrics.StoreClientMetricsPublisher;
import com.flipkart.yak.client.validator.AsyncPayloadValidator;
import com.flipkart.yak.client.validator.AsyncPayloadValidatorImpl;
import com.flipkart.yak.client.validator.RequestValidators;
import com.flipkart.yak.client.validator.RequestValidatorsImpl;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.NoDistribution;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 */

@SuppressWarnings("common-java:DuplicatedBlocks")
public class AsyncStoreClientImpl implements AsyncStoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncStoreClient.class);

  private static final String METRIC_PREFIX_KEY = "com.flipkart.yak.client.async.storeclient.";
  public static final String SEARCH_EXCEPTION = "indexSearchException";
  public static final String RECURSIVE_GET_EXCEPTION = "recursiveGetException";
  public static final String DELETE_ADD_EXCEPTION = "deleteAddException";

  private final Map<String, KeyDistributor> keyDistributorPerTable = new HashMap<>();
  private final MetricRegistry registry;
  private final Optional<Durability> durability;
  private final RequestValidators requestValidators;
  private final ArrayBlockingQueue<IndexDeleteData> deleteIndexes;
  private AsyncConnection connection;
  private AsyncPayloadValidator payloadValidator;
  private ScheduledExecutorService purgeIndexCron;
  private ThreadPoolExecutor executor;
  private final StoreClientMetricsPublisher publisher;

  public AsyncStoreClientImpl(SiteConfig siteConfig, Optional<Map<String, KeyDistributor>> keyDistributorMap,
      int timeoutInSeconds, MetricRegistry registry)
      throws ExecutionException, InterruptedException, TimeoutException, StoreException {
    LOG.info("Initializing Async Storeclient with storeName + " + siteConfig.getStoreName());
    ConfigValidator.validate(siteConfig);
    if (keyDistributorMap.isPresent()) {
      this.keyDistributorPerTable.putAll(keyDistributorMap.get());
    }

    this.deleteIndexes = new ArrayBlockingQueue<>(siteConfig.getIndexPurgeQueueSize());
    this.durability = Optional.ofNullable(siteConfig.getDurabilityThreshold());
    this.registry = registry;
    this.requestValidators =
        new RequestValidatorsImpl(siteConfig.getMaxBatchGetSize(), siteConfig.getMaxBatchDeleteSize());

    this.publisher =
        new StoreClientMetricsPublisher(this.registry, METRIC_PREFIX_KEY + siteConfig.getStoreName() + ".");
    publisher.incrementMetric(StoreClientMetricsPublisher.CONNECTION_INIT);
    try (Timer.Context connectionTimer = publisher.getTimer(StoreClientMetricsPublisher.CONNECTION_TIMER)) {
      Configuration hbaseConf = AsyncStoreClientUtis.buildHbaseConfiguration(siteConfig);
      try {
        if (siteConfig.getHadoopUserName().isPresent()) {
          User user = User.create(UserGroupInformation.createRemoteUser(siteConfig.getHadoopUserName().get()));
          this.connection = ConnectionFactory.createAsyncConnection(hbaseConf, user).get(timeoutInSeconds, TimeUnit.SECONDS);
        } else {
          this.connection = ConnectionFactory.createAsyncConnection(hbaseConf).get(timeoutInSeconds, TimeUnit.SECONDS);
        }
        publisher.incrementMetric(StoreClientMetricsPublisher.CONNECTION_COMPLETE);
      } catch (TimeoutException | ExecutionException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.CONNECTION_GEN_EXCEPTION, e);
        throw e;
      }
    }
    this.executor =
        new ThreadPoolExecutor(siteConfig.getPoolSize(), siteConfig.getPoolSize(), 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new ThreadFactory() {
          private AtomicInteger counter = new AtomicInteger(0);

          @Override public Thread newThread(Runnable r) {
            return new Thread(r, siteConfig.getStoreName() + "-pool-" + counter.getAndIncrement());
          }
        });

    // cron to purge invalid index
    this.purgeIndexCron = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread th = new Thread(r, siteConfig.getStoreName() + "-index-daemon-");
      th.setDaemon(true);
      return th;
    });

    this.purgeIndexCron.scheduleWithFixedDelay(
        new AsyncIndexPurgeDaemon(this.connection, this.deleteIndexes, this.keyDistributorPerTable), 3, 3,
        TimeUnit.SECONDS);

    this.payloadValidator = new AsyncPayloadValidatorImpl(this.connection);
  }

  @SuppressWarnings({"java:S1604", "java:S3776"})
  public CompletableFuture<ResultMap> append(StoreData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.APPEND_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.APPEND_INIT);

    String rowKeyInString = Bytes.toString(getDistributedKey(data.getTableName(), data.getRow(), data.getPartitionKey()));
    CompletableFuture<ResultMap> responseFuture = new CompletableFuture<>();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting append query with row: {}", rowKeyInString);
    }

    Append append = AsyncStoreClientUtis.buildStoreAppend(data, keyDistributorPerTable, durability);
    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    payloadValidator.validate(Arrays.asList(data)).thenComposeAsync(v -> table.append(append))
        .whenCompleteAsync((BiConsumer<Result, Throwable>) (result, error) -> {
      if (error == null && (result == null || result.isEmpty())) {
        error = new StoreDataNotFoundException();
      }
      if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        responseFuture.completeExceptionally(error);
        LOG.error("Failed append query with row: {}, error: {}", rowKeyInString, error.getMessage());
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.APPEND_GEN_EXCEPTION, error);
      } else {
        responseFuture.complete(AsyncStoreClientUtis.buildResultMapFromResult(result));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed append query with row: {}", rowKeyInString);
      }
      publisher.incrementMetric(StoreClientMetricsPublisher.APPEND_COMPLETE);
      timer.close();
    }, executor);
    return responseFuture;
  }

  @SuppressWarnings("java:S1612")
  private CompletableFuture<Void> updateIndexesIfPresent(String indexTableName, List<Put> indexPuts) {
    if (indexPuts.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.INDEX_PUT_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.INDEX_PUT_INIT);

    CompletableFuture<Void> responseFuture = new CompletableFuture<>();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating indexes in table {} for batch Size {}", indexTableName, indexPuts.size());
    }
    AsyncTable indexTable = connection.getTable(TableName.valueOf(indexTableName));
    List<CompletableFuture<Void>> futures = indexTable.put(indexPuts);
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenApply(v -> futures.stream().map(future -> future.join()).collect(Collectors.toList()))
            .whenCompleteAsync((outputs, error) -> {
      if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        responseFuture.completeExceptionally(error);
        LOG.error("Failed index put query with error: {}", error.getMessage());
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.INDEX_PUT_GEN_EXCEPTION, error);
      } else {
        responseFuture.complete(null);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Update completed for indexes in table {} for batch Size {}", indexTableName, indexPuts.size());
      }
      publisher.incrementMetric(StoreClientMetricsPublisher.INDEX_PUT_COMPLETE);
      timer.close();
    }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public CompletableFuture<ResultMap> increment(IncrementData incrementData) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.INCREMENT_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.INCREMENT_INIT);

    CompletableFuture<ResultMap> responseFuture = new CompletableFuture<>();
    String rowKeyInString = Bytes.toString(getDistributedKey(incrementData.getTableName(), incrementData.getRowkey(), incrementData.getPartitionKey()));
    Increment increment = AsyncStoreClientUtis.buildIncrement(incrementData, keyDistributorPerTable);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting Increment query for row: {}", rowKeyInString);
    }

    AsyncTable<AdvancedScanResultConsumer> table = connection.getTable(TableName.valueOf(incrementData.getTableName()));

    table.increment(increment).whenCompleteAsync((BiConsumer<Result, Throwable>) (value, error) -> {
      if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        responseFuture.completeExceptionally(error);
        LOG.error("Increment column query failed with error: {}", error.getMessage());
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.INCREMENT_GEN_EXCEPTION, error);
      } else {
        responseFuture.complete(AsyncStoreClientUtis.buildResultMapFromResult(value));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed Increment Column: {} query for row: {}", incrementData.getClass(), rowKeyInString);
      }
      publisher.incrementMetric(StoreClientMetricsPublisher.INCREMENT_COMPLETE);
      timer.close();
    }, executor);

    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override public CompletableFuture<Void> put(StoreData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.PUT_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.PUT_INIT);

    CompletableFuture<Void> responseFuture = new CompletableFuture<>();
    String rowKeyInString = Bytes.toString(getDistributedKey(data.getTableName(), data.getRow(), data.getPartitionKey()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting put query with row: {}", rowKeyInString);
    }

    StorePuts storePuts = AsyncStoreClientUtis.buildStorePuts(data, keyDistributorPerTable, durability);
    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    payloadValidator.validate(Arrays.asList(data))
        .thenComposeAsync(v -> updateIndexesIfPresent(data.getIndexTableName(), storePuts.indexPuts), executor)
        .thenComposeAsync(v -> table.put(storePuts.entityPut), executor)
        .whenCompleteAsync((BiConsumer<Void, Throwable>) (value, error) -> {
          if (error != null) {
            error = (error instanceof CompletionException) ? error.getCause() : error;
            responseFuture.completeExceptionally(error);
            LOG.error("Put query failed with error: {}", error.getMessage());
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.PUT_GEN_EXCEPTION, error);
          } else {
            responseFuture.complete(value);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed put query with row: {}", rowKeyInString);
          }
          publisher.incrementMetric(StoreClientMetricsPublisher.PUT_COMPLETE);
          timer.close();
        }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings({"java:S1612", "java:S3776"})
  @Override public List<CompletableFuture<Void>> put(List<StoreData> dataList) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.BATCH_PUT_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_PUT_INIT);

    List<CompletableFuture<Void>> responseFutures =
        dataList.stream().map(d -> (new CompletableFuture<Void>())).collect(Collectors.toList());

    CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[responseFutures.size()]))
            .thenApply(v -> responseFutures.stream().map(future -> future.join()).collect(Collectors.toList()))
            .whenCompleteAsync((v, e) -> {
      publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_PUT_COMPLETE);
      timer.close();
    });

    if (dataList.isEmpty()) {
      return responseFutures;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting batch put query with batch size: {}", dataList.size());
    }
    StoreData firstRow = dataList.get(0);
    try {
      requestValidators.validateTableName(firstRow.getTableName(), dataList);
    } catch (RequestValidatorException ex) {
      responseFutures.stream().forEach(future -> future.completeExceptionally(ex));
      LOG.error("Failed batch put query with batch size: {}", dataList.size());
      publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_PUT_GEN_EXCEPTION, ex);
      return responseFutures;
    }

    List<Put> rows = new ArrayList<>();
    List<Put> indexPuts = new ArrayList<>();
    dataList.stream().forEachOrdered(storeData -> {
      StorePuts storePuts = AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorPerTable, durability);
      indexPuts.addAll(storePuts.indexPuts);
      rows.add(storePuts.entityPut);
    });
    payloadValidator.validate(dataList)
        .thenComposeAsync(value -> updateIndexesIfPresent(firstRow.getIndexTableName(), indexPuts), executor)
        .thenComposeAsync(value -> CompletableFuture.runAsync(() -> {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Building batch put query with batch size: {}", dataList.size());
          }
          AsyncTable table = connection.getTable(TableName.valueOf(firstRow.getTableName()));
          List<CompletableFuture<Void>> futures = table.put(rows);
          Iterator<CompletableFuture<Void>> iterator = responseFutures.iterator();
          futures.stream().forEachOrdered(future -> {
            CompletableFuture<Void> responseFuture =
                (iterator.hasNext()) ? iterator.next() : (new CompletableFuture<Void>());
            future.whenCompleteAsync((aVoid, error) -> {
              if (error != null) {
                error = (error instanceof CompletionException) ? error.getCause() : error;
                responseFuture.completeExceptionally(error);
                LOG.error("Failed batch put query with error: {}", error.getMessage());
                publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_PUT_GEN_EXCEPTION, error);
              } else {
                responseFuture.complete(value);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Completed batch put query with batch size: {}", dataList.size());
              }
            }, executor);
          });
        })).whenCompleteAsync((value, error) -> {
      responseFutures.stream().forEach(future -> future.completeExceptionally(error));
      publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_PUT_GEN_EXCEPTION, error);
    });
    return responseFutures;
  }

  /**
   * {@inheritDoc}
   */
  @Override public CompletableFuture<Boolean> checkAndPut(CheckAndStoreData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.CAS_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.CAS_INIT);

    CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();
    String rowKeyInString = Bytes.toString(getDistributedKey(data.getTableName(), data.getRow(), data.getPartitionKey()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting check and put query with row: {}", rowKeyInString);
    }

    StorePuts storePuts = AsyncStoreClientUtis.buildStorePuts(data, keyDistributorPerTable, durability);
    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    CheckVerifyData vdata = data.getVerifyData();
    byte[] rowKey = storePuts.entityPut.getRow();

    AsyncTable.CheckAndMutateBuilder checkAndMutateBuilder =
            table.checkAndMutate(rowKey, vdata.getCf().getBytes()).qualifier(vdata.getQualifier().getBytes());
    if (vdata.getData() == null) {
        checkAndMutateBuilder = checkAndMutateBuilder.ifNotExists();
    } else {
      checkAndMutateBuilder = checkAndMutateBuilder.ifMatches(vdata.getCompareOperator(), vdata.getData());
    }
    AsyncTable.CheckAndMutateBuilder finalCheckAndMutateBuilder = checkAndMutateBuilder;
    payloadValidator.validate(Arrays.asList(data))
        .thenComposeAsync(v -> updateIndexesIfPresent(data.getIndexTableName(), storePuts.indexPuts), executor)
        .thenComposeAsync(v -> finalCheckAndMutateBuilder.thenPut(storePuts.entityPut))
        .whenCompleteAsync((value, error) -> {
          if (error != null) {
            error = (error instanceof CompletionException) ? error.getCause() : error;
            responseFuture.completeExceptionally(error);
            LOG.error("Failed check and put query with row: {}, error: {}", rowKeyInString, error.getMessage());
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.CAS_GEN_EXCEPTION, error);
          } else {
            responseFuture.complete(value);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed check and put query with row: {}", rowKeyInString);
          }
          publisher.incrementMetric(StoreClientMetricsPublisher.CAS_COMPLETE);
          timer.close();
        }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override public CompletableFuture<Boolean> checkAndDelete(CheckAndDeleteData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.CHECK_DELETE_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.CHECK_DELETE_INIT);

    byte[] key = getDistributedKey(data.getTableName(), data.getRow(), data.getPartitionKey());
    String rowKeyInString = Bytes.toString(key);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting check and delete query with row: {}", rowKeyInString);
    }

    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, key);
    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    CheckVerifyData vdata = data.getVerifyData();
    AsyncTable.CheckAndMutateBuilder builder =
            table.checkAndMutate(key, vdata.getCf().getBytes()).qualifier(vdata.getQualifier().getBytes());
    if (vdata.getData() == null) {
      builder = builder.ifNotExists();
    } else {
      builder = builder.ifMatches(vdata.getCompareOperator(), vdata.getData());
    }

    return builder.thenDelete(delete).whenCompleteAsync((value, error) -> {
          if (error != null) {
            error = (error instanceof CompletionException) ? error.getCause() : error;
            LOG.error("Failed check and delete query with row: {}, error: {}", rowKeyInString, error.getMessage());
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.CHECK_DELETE_GEN_EXCEPTION, error);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed check and delete query with row: {}", rowKeyInString);
          }
          publisher.incrementMetric(StoreClientMetricsPublisher.CHECK_DELETE_COMPLETE);
          timer.close();
        }, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override public CompletableFuture<Void> delete(DeleteData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.DELETE_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.DELETE_INIT);

    byte[] key = getDistributedKey(data.getTableName(), data.getRow(), data.getPartitionKey());
    String rowKeyInString = Bytes.toString(key);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting delete query with row: {}", rowKeyInString);
    }

    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, key);
    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    return table.delete(delete).whenCompleteAsync((BiConsumer<Void, Throwable>) (value, error) -> {
      if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        LOG.error("Failed delete query with row: {}, error: {}", rowKeyInString, error.getMessage());
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.DELETE_GEN_EXCEPTION, error);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed delete query with row: {}", rowKeyInString);
      }
      publisher.incrementMetric(StoreClientMetricsPublisher.DELETE_COMPLETE);
      timer.close();
    }, executor);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("java:S1612")
  @Override public List<CompletableFuture<Void>> delete(List<DeleteData> dataList) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.BATCH_DELETE_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_DELETE_INIT);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting bulk delete query with size: {}", dataList.size());
    }
    List<CompletableFuture<Void>> responseFutures =
        dataList.stream().map(d -> (new CompletableFuture<Void>())).collect(Collectors.toList());
    CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[responseFutures.size()]))
            .thenApply(v -> responseFutures.stream().map(future -> future.join()).collect(Collectors.toList()))
            .whenCompleteAsync((v, e) -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed batch delete query with batch size: {}", dataList.size());
      }
      publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_DELETE_COMPLETE);
      timer.close();
    });

    if (dataList.isEmpty()) {
      return responseFutures;
    }

    DeleteData firstData = dataList.get(0);
    try {
      requestValidators.validateBatchDeleteSize(dataList);
      requestValidators.validateTableName(firstData.getTableName(), dataList);
    } catch (RequestValidatorException ex) {
      publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_DELETE_GEN_EXCEPTION, ex);
      responseFutures.stream().forEach(future -> future.completeExceptionally(ex));
      return responseFutures;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Building bulk delete query with size: {}", dataList.size());
    }
    List<Delete> hbaseDeletes = new ArrayList<>();
    dataList.stream().forEachOrdered(del -> {
      byte[] key = getDistributedKey(del.getTableName(), del.getRow(), del.getPartitionKey());
      hbaseDeletes.add(AsyncStoreClientUtis.buildDeleteQuery(del, key));
    });

    AsyncTable table = connection.getTable(TableName.valueOf(firstData.getTableName()));
    List<CompletableFuture<Void>> futures = table.delete(hbaseDeletes);
    futures.forEach(future -> future.whenCompleteAsync((value, error) -> {
      if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        LOG.error("Failed batch delete query with error: {}", error.getMessage());
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_DELETE_GEN_EXCEPTION, error);
      }
    }));
    return futures;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> mutate(MutateData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.MUTATE_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.MUTATE_INIT);

    CompletableFuture<Void> responseFuture = new CompletableFuture<>();
    String rowKeyInString = Bytes.toString(getDistributedKey(data.getTableName(), data.getRow()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting mutate query with row: {}", rowKeyInString);
    }
    Mutation mutation = null;
    try {
      mutation = AsyncStoreClientUtis.buildMutation(data, keyDistributorPerTable, durability);
    } catch (IOException e) {
      responseFuture.completeExceptionally(e);
      LOG.error("mutate query failed with error: {}", e.getMessage());
      publisher.incrementErrorMetric(StoreClientMetricsPublisher.MUTATE_GEN_EXCEPTION, e);
      return responseFuture;
    }
    final Mutation finalMutation = mutation;
    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    payloadValidator.validate(data.getStoreDataList())
        .thenComposeAsync(v -> updateIndexesIfPresent(data.getIndexTableName(), finalMutation.indexPuts), executor)
        .thenComposeAsync(v -> table.mutateRow(finalMutation.rowMutations), executor)
        .whenCompleteAsync((BiConsumer<Void, Throwable>) (value, error) -> {
          if (error != null) {
            error = (error instanceof CompletionException) ? error.getCause() : error;
            responseFuture.completeExceptionally(error);
            LOG.error("mutate query failed with error: {}", error.getMessage());
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.MUTATE_GEN_EXCEPTION, error);
          } else {
            responseFuture.complete(value);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed mutate query with row: {}", rowKeyInString);
          }
          publisher.incrementMetric(StoreClientMetricsPublisher.MUTATE_COMPLETE);
          timer.close();
        }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Boolean> checkAndMutate(CheckAndMutateData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.CHECK_MUTATE_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.CHECK_MUTATE_INIT);

    CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();
    String rowKeyInString = Bytes.toString(getDistributedKey(data.getTableName(), data.getRow()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting check and mutate query with row: {}", rowKeyInString);
    }
    Mutation mutation = null;
    try {
      mutation = AsyncStoreClientUtis.buildMutation(data, keyDistributorPerTable, durability);
    } catch (IOException e) {
      responseFuture.completeExceptionally(e);
      LOG.error("Failed check and mutate query with row: {}, error: {}", rowKeyInString, e.getMessage());
      publisher.incrementErrorMetric(StoreClientMetricsPublisher.CHECK_MUTATE_GEN_EXCEPTION, e);
      return responseFuture;
    }
    final Mutation finalMutation = mutation;
    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    CheckVerifyData vdata = data.getVerifyData();
    byte[] rowKey = finalMutation.rowMutations.getRow();

    AsyncTable.CheckAndMutateBuilder checkAndMutateBuilder =
        table.checkAndMutate(rowKey, vdata.getCf().getBytes()).qualifier(vdata.getQualifier().getBytes());
    if (vdata.getData() == null) {
      checkAndMutateBuilder = checkAndMutateBuilder.ifNotExists();
    } else {
      checkAndMutateBuilder = checkAndMutateBuilder.ifMatches(vdata.getCompareOperator(), vdata.getData());
    }
    AsyncTable.CheckAndMutateBuilder finalCheckAndMutateBuilder = checkAndMutateBuilder;
    payloadValidator.validate(data.getStoreDataList())
        .thenComposeAsync(v -> updateIndexesIfPresent(data.getIndexTableName(), finalMutation.indexPuts), executor)
        .thenComposeAsync(v -> finalCheckAndMutateBuilder.thenMutate(finalMutation.rowMutations))
        .whenCompleteAsync((value, error) -> {
          if (error != null) {
            error = (error instanceof CompletionException) ? error.getCause() : error;
            responseFuture.completeExceptionally(error);
            LOG.error("Failed check and mutate query with row: {}, error: {}", rowKeyInString, error.getMessage());
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.CHECK_MUTATE_GEN_EXCEPTION, error);
          } else {
            responseFuture.complete(value);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed check and mutate query with row: {}", rowKeyInString);
          }
          publisher.incrementMetric(StoreClientMetricsPublisher.CHECK_MUTATE_COMPLETE);
          timer.close();
        }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> batch(BatchData data) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.BATCH_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_INIT);

    CompletableFuture<Void> responseFuture = new CompletableFuture<>();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting batch query with {} puts and {} deletes", data.getStoreDataList().size(),
          data.getDeleteDataList().size());
    }

    try {
      requestValidators.validateTableName(data.getTableName(), data.getStoreDataList());
      requestValidators.validateTableName(data.getTableName(), data.getDeleteDataList());
    } catch (RequestValidatorException ex) {
      LOG.error("Failed batch query with {} puts and {} deletes", data.getStoreDataList().size(), data.getDeleteDataList().size(), ex);
      publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_GEN_EXCEPTION, ex);
      responseFuture.completeExceptionally(ex);
      timer.close();
      return responseFuture;
    }

    AsyncStoreClientUtis.BatchActions batchActions = AsyncStoreClientUtis.buildBatch(data, keyDistributorPerTable, durability);

    AsyncTable table = connection.getTable(TableName.valueOf(data.getTableName()));
    List<StoreData> validationList = new ArrayList<>(data.getStoreDataList());
    List<Row> hbaseActions = batchActions.actions;
    List<Put> hbaseIndexPuts = batchActions.indexPuts;
    payloadValidator.validate(validationList)
            .thenComposeAsync(v -> updateIndexesIfPresent(data.getIndexTableName(), hbaseIndexPuts), executor)
            .thenComposeAsync(v -> {
              List<CompletableFuture<Void>> futures = table.batch(hbaseActions);
              return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            }, executor)
            .whenCompleteAsync((BiConsumer<Void, Throwable>) (value, error) -> {
              if (error != null) {
                error = (error instanceof CompletionException) ? error.getCause() : error;
                responseFuture.completeExceptionally(error);
                LOG.error("Batch query failed with error: {}", error.getMessage());
                publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_GEN_EXCEPTION, error);
              } else {
                responseFuture.complete(value);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Completed batch query with {} puts and {} deletes", data.getStoreDataList().size(),
                        data.getDeleteDataList().size());
              }
              publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_COMPLETE);
              timer.close();
            }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override public <T extends GetRow> CompletableFuture<ResultMap> get(T row) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.GET_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.GET_INIT);

    CompletableFuture<ResultMap> responseFuture = new CompletableFuture<>();
    String rowKeyInString = Bytes.toString(getDistributedKey(row.getTableName(), row.getKey(), row.getPartitionKey()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting GetRow: {} query for row: {}", row.getClass(), rowKeyInString);
    }

    Get hbaseGet = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorPerTable).get(0);
    AsyncTable table = connection.getTable(TableName.valueOf(row.getTableName()));
    table.get(hbaseGet).whenCompleteAsync((BiConsumer<Result, Throwable>) (value, error) -> {
      if (error == null && (value == null || value.isEmpty())) {
        error = new StoreDataNotFoundException();
      } else if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        LOG.error("Failure GetRow: {} query for row: {}, error: {}", row.getClass(), rowKeyInString,
            error.getMessage());
      }
      if (error != null) {
        responseFuture.completeExceptionally(error);
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.GET_GEN_EXCEPTION, error);
      } else {
        responseFuture.complete(AsyncStoreClientUtis.buildResultMapFromResult(value));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed GetRow: {} query for row: {}", row.getClass(), rowKeyInString);
      }
      publisher.incrementMetric(StoreClientMetricsPublisher.GET_COMPLETE);
      timer.close();
    }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings({"java:S1612", "java:S3776"})
  @Override public List<CompletableFuture<ResultMap>> get(List<? extends GetRow> rows) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.BATCH_GET_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_GET_INIT);

    List<CompletableFuture<ResultMap>> responseFutures =
        rows.stream().map(r -> (new CompletableFuture<ResultMap>())).collect(Collectors.toList());
    CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[responseFutures.size()]))
            .thenApply(v -> responseFutures.stream().map(future -> future.join()).collect(Collectors.toList()))
            .whenCompleteAsync((v, e) -> {
      publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_GET_COMPLETE);
      timer.close();
    });

    if (rows.isEmpty()) {
      return responseFutures;
    }

    try {
      requestValidators.validateBatchGetSize(rows);
      requestValidators.validateTableName(rows.get(0).getTableName(), rows);
    } catch (RequestValidatorException ex) {
      publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_GET_GEN_EXCEPTION, ex);
      responseFutures.stream().forEach(future -> future.completeExceptionally(ex));
      return responseFutures;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting get queries with size: {}", rows.size());
    }
    GetRow firstRow = rows.get(0);
    List<Get> hbaseGets = AsyncStoreClientUtis.buildGets(rows, keyDistributorPerTable);
    AsyncTable table = connection.getTable(TableName.valueOf(firstRow.getTableName()));
    List<CompletableFuture<Result>> futures = table.get(hbaseGets);
    Iterator<CompletableFuture<ResultMap>> iterator = responseFutures.iterator();
    futures.stream().forEachOrdered(future -> {
      CompletableFuture<ResultMap> responseFuture =
          (iterator.hasNext()) ? iterator.next() : new CompletableFuture<>();
      future.whenCompleteAsync((value, error) -> {
        if (error == null && (value == null || value.isEmpty())) {
          error = new StoreDataNotFoundException();
        } else if (error != null) {
          error = (error instanceof CompletionException) ? error.getCause() : error;
          LOG.error("Failure get queries for GetRow: {} with size: {}, error: {}", firstRow.getClass(), rows.size(),
              error.getMessage());
        }
        if (error != null) {
          publisher.incrementErrorMetric(StoreClientMetricsPublisher.BATCH_GET_GEN_EXCEPTION, error);
          responseFuture.completeExceptionally(error);
        } else {
          responseFuture.complete(AsyncStoreClientUtis.buildResultMapFromResult(value));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Completed get queries for GetRow: {} with size: {}", firstRow.getClass(), rows.size());
        }
        publisher.incrementMetric(StoreClientMetricsPublisher.BATCH_GET_COMPLETE);
        timer.close();
      }, executor);
    });
    return responseFutures;
  }

  @SuppressWarnings("java:S3776")
  private CompletableFuture<IndexResponse> searchIndex(IndexLookup indexLookup, KeyDistributor keyDistributor) {
    CompletableFuture<IndexResponse> responseFuture = new CompletableFuture<>();

    byte[] key = keyDistributor.enrichKey(indexLookup.getKey());
    String rowKeyInString = Bytes.toString(key);
    List<Tupple> rowKeys = new ArrayList<>();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting index search for index key: " + rowKeyInString);
    }
    AsyncTable table = connection.getTable(TableName.valueOf(indexLookup.getIndexTableName()));
    Get indexGet = new Get(key);
    if (indexLookup.getIndexAtt() instanceof SimpleIndexAttributes) {
      indexGet.addColumn(IndexConstants.INDEX_CF, IndexConstants.INDEX_COL);
    } else {
      indexGet.addFamily(IndexConstants.INDEX_CF);
    }

    table.get(indexGet).whenCompleteAsync((BiConsumer<Result, Throwable>) (value, error) -> {
      if ((error == null) && (value == null || value.isEmpty())) {
        error = new StoreDataNotFoundException();
      } else if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        LOG.error("Failed index search for index key: {}, error: {}", rowKeyInString, error.getMessage());
      }
      if (error != null) {
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.INDEX_GET_GEN_EXCEPTION, error);
        responseFuture.completeExceptionally(error);
      } else {
        if (indexLookup.getIndexAtt() instanceof SimpleIndexAttributes) {
          int start = 0;
          int stop = 1;
          byte[] rowKey = value.getValue(IndexConstants.INDEX_CF, IndexConstants.INDEX_COL);
          if (rowKey == null) {
            responseFuture.completeExceptionally(new StoreDataNotFoundException());
          } else {
            rowKeys.add(new Tupple(rowKey, System.currentTimeMillis()));
            responseFuture.complete(new IndexResponse(rowKeys, start, stop));
          }
        } else {
          NavigableMap<byte[], byte[]> colMap = value.getFamilyMap(IndexConstants.INDEX_CF);
          for (Map.Entry<byte[], byte[]> colEntry : colMap.entrySet()) {
            long val = Bytes.toLong(colEntry.getValue());
            rowKeys.add(new Tupple(colEntry.getKey(), val));
          }
          AppendOnlyIndexAttributes att = (AppendOnlyIndexAttributes) indexLookup.getIndexAtt();
          rowKeys.sort(Tupple.COMPARATOR);
          if (rowKeys.isEmpty()) {
            responseFuture.completeExceptionally(new StoreDataNotFoundException());
          }
          if (att.getOffset() >= rowKeys.size()) {
            responseFuture.completeExceptionally(new StoreException("Invalid offset argument, offset > rowKeySize"));
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.INDEX_GET_GEN_EXCEPTION, SEARCH_EXCEPTION);
          }
          int start = att.getOffset();
          int stop = start + att.getPageSize();
          if (stop > rowKeys.size()) {
            stop = rowKeys.size();
          }
          responseFuture.complete(new IndexResponse(rowKeys, start, stop));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed index search for index key: {}", rowKeyInString);
      }
    }, executor);

    return responseFuture;
  }

  @SuppressWarnings("java:S3776")
  private <T> CompletableFuture<RecurseData<T>> recursiveGet(int start, int stop, RecurseData<T> recurseData) {
    CompletableFuture<RecurseData<T>> responseFuture = new CompletableFuture<>();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting recursive get for search index with start: {}, stop: {}", start, stop);
    }
    // base case
    if (start >= recurseData.rowKeys.size()) {
      responseFuture.complete(recurseData);
      return responseFuture;
    }
    if (recurseData.output.size() == recurseData.pageSize) {
      // definsive case may not be needed
      responseFuture.complete(recurseData);
      return responseFuture;
    }
    // avoid IndexOUtOf Bounds
    if (stop > recurseData.rowKeys.size()) {
      stop = recurseData.rowKeys.size();
    }

    // build Gets on entityId for single batch call
    List<Get> gets = new ArrayList<>();
    for (int i = start; i < stop; i++) {
      byte[] key = recurseData.keyDistributor.enrichKey(recurseData.rowKeys.get(i).key);
      Get entityGet = new Get(key);
      recurseData.updateGet(entityGet);
      gets.add(entityGet);
    }

    // check for invalid Index using missedCount
    AsyncTable table = connection.getTable(TableName.valueOf(recurseData.tableName));
    List<CompletableFuture<Result>> futures = table.get(gets);
    final int stopFinal = stop;
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenApply(v -> futures.stream().map(future -> future.join()).collect(Collectors.toList()))
            .whenCompleteAsync((results, throwable) -> {
      if (throwable != null) {
        responseFuture.completeExceptionally(throwable);
      } else {
        int missedCount = 0;
        for (int index = 0; index < results.size(); index += 1) {
          Result result = results.get(index);
          if (result.isEmpty() || !result.containsColumn(recurseData.cf, recurseData.indexKey)) {
            LOG.error("Invalid index {}", result.isEmpty());
            missedCount++;
            try {
              deleteIndexes.add(recurseData.buildDeleteData(gets.get(index))); // mark invalid indexes for deletion
            } catch (IllegalStateException ex) {
              LOG.error("Failed to insert into to deleteIndexQueue as queue is full", ex); // ignore this
              publisher
                  .incrementErrorMetric(StoreClientMetricsPublisher.INDEX_GET_GEN_EXCEPTION, DELETE_ADD_EXCEPTION);
            }
          } else {
            recurseData.updateOutput(result); // build response
          }
        }
        // recurse to meet required pageSize in case of missedCounts
        if (missedCount > 0) {
          recursiveGet(stopFinal, stopFinal + missedCount, recurseData).whenCompleteAsync((value, error) -> {
            if (error != null) {
              responseFuture.completeExceptionally(error);
              publisher
                  .incrementErrorMetric(StoreClientMetricsPublisher.INDEX_GET_GEN_EXCEPTION, RECURSIVE_GET_EXCEPTION);
            } else {
              responseFuture.complete(value);
            }
          });
        } else {
          responseFuture.complete(recurseData);
        }
      }
    }, executor);

    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override public CompletableFuture<List<Cell>> getByIndex(GetCellByIndex row) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.INDEX_GET_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.INDEX_GET_INIT);

    CompletableFuture<List<Cell>> responseFuture = new CompletableFuture<>();

    KeyDistributor keyDistributor = keyDistributorPerTable.getOrDefault(row.getTableName(), NoDistribution.INSTANCE);
    KeyDistributor indexKeyDistributor = (keyDistributorPerTable.containsKey(row.getIndexTableName())) ?
        keyDistributorPerTable.get(row.getIndexTableName()) :
        keyDistributor;

    String rowKeyAsString = Bytes.toString(indexKeyDistributor.enrichKey(row.getKey()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Building GetCellByIndex queries for index key: {}", rowKeyAsString);
    }
    searchIndex(row, indexKeyDistributor).thenComposeAsync(
        indexResponse -> recursiveGet(indexResponse.start, indexResponse.stop,
            new RecurseData<Cell>(indexResponse, row, keyDistributor, indexKeyDistributor)), executor)
        .whenCompleteAsync((recursiveData, error) -> {
          if (error == null && recursiveData.output.isEmpty()) {
            error = new StoreDataNotFoundException();
          }
          if (error != null) {
            error = (error instanceof CompletionException) ? error.getCause() : error;
            responseFuture.completeExceptionally(error);
            LOG.error("Failed GetCellByIndex query for index key: {}, error: {}", rowKeyAsString, error.getMessage());
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.INDEX_GET_GEN_EXCEPTION, error);
          } else {
            responseFuture.complete(recursiveData.output);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed GetCellByIndex queries for index key: {}", rowKeyAsString);
          }
          publisher.incrementMetric(StoreClientMetricsPublisher.INDEX_GET_COMPLETE);
          timer.close();
        }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override public CompletableFuture<List<ColumnsMap>> getByIndex(GetColumnsMapByIndex row) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.INDEX_GET_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.INDEX_GET_INIT);

    CompletableFuture<List<ColumnsMap>> responseFuture = new CompletableFuture<>();
    KeyDistributor keyDistributor = keyDistributorPerTable.getOrDefault(row.getTableName(), NoDistribution.INSTANCE);
    KeyDistributor indexKeyDistributor = (keyDistributorPerTable.containsKey(row.getIndexTableName())) ?
        keyDistributorPerTable.get(row.getIndexTableName()) :
        keyDistributor;

    String rowKeyAsString = Bytes.toString(indexKeyDistributor.enrichKey(row.getKey()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Building GetColumnsMapByIndex queries for index key: {}", rowKeyAsString);
    }
    searchIndex(row, indexKeyDistributor).thenComposeAsync(
        indexResponse -> recursiveGet(indexResponse.start, indexResponse.stop,
            new RecurseData<ColumnsMap>(indexResponse, row, keyDistributor, indexKeyDistributor)), executor)
        .whenCompleteAsync((recursiveData, error) -> {
          if (error == null && recursiveData.output.isEmpty()) {
            error = new StoreDataNotFoundException();
          } else if (error != null) {
            error = (error instanceof CompletionException) ? error.getCause() : error;
            LOG.error("Failed GetColumnsMapByIndex query for index key: {}, error: {}", rowKeyAsString,
                error.getMessage());
          }
          if (error != null) {
            responseFuture.completeExceptionally(error);
            publisher.incrementErrorMetric(StoreClientMetricsPublisher.INDEX_GET_GEN_EXCEPTION, error);
          } else {
            responseFuture.complete(recursiveData.output);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed GetColumnsMapByIndex queries for index key: {}", rowKeyAsString);
          }
          publisher.incrementMetric(StoreClientMetricsPublisher.INDEX_GET_COMPLETE);
          timer.close();
        }, executor);
    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override public CompletableFuture<Map<String, ResultMap>> scan(ScanData scanData) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(StoreClientMetricsPublisher.SCAN_TIMER);
    publisher.incrementMetric(StoreClientMetricsPublisher.SCAN_INIT);

    CompletableFuture<Map<String, ResultMap>> responseFuture = new CompletableFuture<>();
    Scan scan;
    try {
      scan = AsyncStoreClientUtis.buildScan(scanData, keyDistributorPerTable);
    } catch (StoreException e) {
      LOG.error("Failed to build scanner object with: {}, error: {}", scanData, e.getMessage(), e);
      responseFuture.completeExceptionally(e);
      return responseFuture;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting scan with scan query: " + scan);
    }

    AsyncTable table = connection.getTable(TableName.valueOf(scanData.getTableName()));
    table.scanAll(scan).whenCompleteAsync((BiConsumer<List<Result>, Throwable>) (results, error) -> {
      if (error != null) {
        error = (error instanceof CompletionException) ? error.getCause() : error;
        LOG.error("Failure to scan with scan object : {} error: {}", scan, error.getMessage(), error);
        responseFuture.completeExceptionally(error);
        publisher.incrementErrorMetric(StoreClientMetricsPublisher.SCAN_GEN_EXCEPTION, error);
      } else {
        responseFuture.complete(results.stream().collect(Collectors.toMap(result -> Bytes.toString(result.getRow()),
                result -> AsyncStoreClientUtis.buildResultMapFromResult(result), (u, v) ->  u, LinkedHashMap::new)));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed scan with scan query: " + scan);
      }
      publisher.incrementMetric(StoreClientMetricsPublisher.SCAN_COMPLETE);
      timer.close();
    }, executor);

    return responseFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override public byte[] getDistributedKey(String tableName, byte[] rowKey, byte[] partitionKey) {
    return AsyncStoreClientUtis.getDistributedKey(keyDistributorPerTable, tableName, rowKey, partitionKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override public AsyncConnection getConnection() {
    return connection;
  }

  @Override
  public byte[] getDistributedKey(String tableName, byte[] rowKey) {
    return AsyncStoreClientUtis.getDistributedKey(keyDistributorPerTable, tableName, rowKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override public void shutdown() throws IOException, InterruptedException {
    connection.close();
    purgeIndexCron.shutdown();
    purgeIndexCron.awaitTermination(1, TimeUnit.MINUTES);
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }
}
