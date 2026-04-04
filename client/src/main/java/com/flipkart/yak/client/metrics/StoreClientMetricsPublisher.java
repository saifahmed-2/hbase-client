package com.flipkart.yak.client.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

public class StoreClientMetricsPublisher {
  private static final String METRIC_REQUESTS_TYPE = "requests.";
  private static final String METRIC_EXCEPTIONS_TYPE = "exceptions.";
  private static final String METRIC_METHOD_APPEND_NAME = "append.";
  private static final String METRIC_METHOD_PUT_NAME = "put.";
  private static final String METRIC_METHOD_CAS_NAME = "cas.";
  private static final String METRIC_METHOD_GET_NAME = "get.";
  private static final String METRIC_METHOD_SCAN_NAME = "scan.";
  private static final String METRIC_METHOD_DELETE_NAME = "delete.";
  private static final String METRIC_METHOD_CHECK_DELETE_NAME = "checkDelete.";
  private static final String METRIC_METHOD_BATCH_PUT_NAME = "batchPut.";
  private static final String METRIC_METHOD_BATCH_DELETE_NAME = "batchDelete.";
  private static final String METRIC_METHOD_BATCH_GET_NAME = "batchGet.";
  private static final String METRIC_METHOD_BATCH_NAME = "batch.";
  private static final String METRIC_METHOD_INDEX_PUT_NAME = "indexPut.";
  private static final String METRIC_METHOD_INDEX_GET_NAME = "indexGet.";
  private static final String METRIC_METHOD_MUTATE_NAME = "mutate.";
  private static final String METRIC_METHOD_CHECK_AND_MUTATE_NAME = "checkMutate.";
  private static final String METRIC_METHOD_INCREMENT_NAME = "increment.";
  private static final String METRIC_METHOD_CONNECTION_NAME = "connection.";
  private static final String METRIC_STORE_EXCEPTIONS = "storeExceptions";
  private static final String METRIC_TIMER = "duration";
  private static final String METRIC_INIT = "init";
  private static final String METRIC_COMPLETE = "complete";

  public static final String INDEX_PUT_INIT = "indexPutInit";
  public static final String INDEX_PUT_COMPLETE = "indexPutComplete";
  public static final String INDEX_PUT_GEN_EXCEPTION = "indexPutGenericException";

  public static final String APPEND_INIT = "appendInit";
  public static final String APPEND_COMPLETE = "appendComplete";
  public static final String APPEND_GEN_EXCEPTION = "appendGenericException";

  public static final String PUT_INIT = "putInit";
  public static final String PUT_COMPLETE = "putComplete";
  public static final String PUT_GEN_EXCEPTION = "putGenericException";

  public static final String BATCH_PUT_INIT = "batchPutInit";
  public static final String BATCH_PUT_COMPLETE = "batchPutComplete";
  public static final String BATCH_PUT_GEN_EXCEPTION = "batchPutGenericException";

  public static final String BATCH_INIT = "batchInit";
  public static final String BATCH_COMPLETE = "batchComplete";
  public static final String BATCH_GEN_EXCEPTION = "batchGenericException";

  public static final String CAS_INIT = "casInit";
  public static final String CAS_COMPLETE = "casComplete";
  public static final String CAS_GEN_EXCEPTION = "casGenericException";

  public static final String CHECK_DELETE_INIT = "checkDeleteInit";
  public static final String CHECK_DELETE_COMPLETE = "checkDeleteComplete";
  public static final String CHECK_DELETE_GEN_EXCEPTION = "checkDeleteGenericException";

  public static final String DELETE_INIT = "deleteInit";
  public static final String DELETE_COMPLETE = "deleteComplete";
  public static final String DELETE_GEN_EXCEPTION = "deleteGenericException";

  public static final String BATCH_DELETE_INIT = "batchDeleteInit";
  public static final String BATCH_DELETE_COMPLETE = "batchDeleteComplete";
  public static final String BATCH_DELETE_GEN_EXCEPTION = "batchDeleteGenericException";

  public static final String GET_INIT = "getInit";
  public static final String GET_COMPLETE = "getComplete";
  public static final String GET_GEN_EXCEPTION = "getGenericException";

  public static final String SCAN_INIT = "scanInit";
  public static final String SCAN_COMPLETE = "scanComplete";
  public static final String SCAN_GEN_EXCEPTION = "scanGenericException";

  public static final String BATCH_GET_INIT = "batchGetInit";
  public static final String BATCH_GET_COMPLETE = "batchGetComplete";
  public static final String BATCH_GET_GEN_EXCEPTION = "batchGetGenericException";

  public static final String INDEX_GET_INIT = "indexGetInit";
  public static final String INDEX_GET_COMPLETE = "indexGetComplete";
  public static final String INDEX_GET_GEN_EXCEPTION = "indexGetGenericException";

  public static final String CHECK_MUTATE_INIT = "checkMutateInit";
  public static final String CHECK_MUTATE_COMPLETE = "checkMutateComplete";
  public static final String CHECK_MUTATE_GEN_EXCEPTION = "checkMutateGenericException";

  public static final String MUTATE_INIT = "mutateInit";
  public static final String MUTATE_COMPLETE = "mutateComplete";
  public static final String MUTATE_GEN_EXCEPTION = "mutateGenericException";

  public static final String INCREMENT_INIT = "incrementInit";
  public static final String INCREMENT_COMPLETE = "incrementComplete";
  public static final String INCREMENT_GEN_EXCEPTION = "incrementGenericException";

  public static final String CONNECTION_INIT = "connectionInit";
  public static final String CONNECTION_COMPLETE = "connectionComplete";
  public static final String CONNECTION_GEN_EXCEPTION = "connectionGenericException";

  public static final String APPEND_TIMER = "appendTimer";
  public static final String INDEX_PUT_TIMER = "indexPutTimer";
  public static final String PUT_TIMER = "putTimer";
  public static final String BATCH_PUT_TIMER = "batchPutTimer";
  public static final String BATCH_TIMER = "batchTimer";
  public static final String CAS_TIMER = "casTimer";
  public static final String GET_TIMER = "getTimer";
  public static final String SCAN_TIMER = "scanTimer";
  public static final String BATCH_GET_TIMER = "batchGetTimer";
  public static final String DELETE_TIMER = "deleteTimer";
  public static final String BATCH_DELETE_TIMER = "batchDeleteTimer";
  public static final String CHECK_DELETE_TIMER = "checkDeleteTimer";
  public static final String INDEX_GET_TIMER = "indexGetTimer";
  public static final String MUTATE_TIMER = "mutateTimer";
  public static final String CHECK_MUTATE_TIMER = "checkMutateTimer";
  public static final String INCREMENT_TIMER = "incrementTimer";
  public static final String CONNECTION_TIMER = "connectionTimer";

  public static final String ACTIVE_THREAD_COUNT = "activeThreadCount";
  public static final String QUEUE_SIZE = "pendingQueueSize";
  public static final String THREAD_COUNT = "totalThreadCount";

  private Map<String, String> meterMapping = new HashMap<>();
  private Map<String, String> errorMapping = new HashMap<>();
  private Map<String, String> timerMapping = new HashMap<>();
  private Map<String, String> counterMapping = new HashMap<>();
  private MetricsPublisherImpl publisher;

  @SuppressWarnings({"java:S1905", "java:S2864", "common-java:DuplicatedBlocks"})
  public StoreClientMetricsPublisher(MetricRegistry registry, String prefix) {
    publisher = new MetricsPublisherImpl(registry, prefix);

    counterMapping.put(ACTIVE_THREAD_COUNT, ACTIVE_THREAD_COUNT);
    counterMapping.put(QUEUE_SIZE, QUEUE_SIZE);
    counterMapping.put(THREAD_COUNT, THREAD_COUNT);

    timerMapping.put(APPEND_TIMER, METRIC_METHOD_APPEND_NAME + METRIC_TIMER);
    timerMapping.put(INDEX_PUT_TIMER, METRIC_METHOD_INDEX_PUT_NAME + METRIC_TIMER);
    timerMapping.put(PUT_TIMER, METRIC_METHOD_PUT_NAME + METRIC_TIMER);
    timerMapping.put(BATCH_PUT_TIMER, METRIC_METHOD_BATCH_PUT_NAME + METRIC_TIMER);
    timerMapping.put(BATCH_TIMER, METRIC_METHOD_BATCH_NAME + METRIC_TIMER);
    timerMapping.put(CAS_TIMER, METRIC_METHOD_CAS_NAME + METRIC_TIMER);
    timerMapping.put(GET_TIMER, METRIC_METHOD_GET_NAME + METRIC_TIMER);
    timerMapping.put(SCAN_TIMER, METRIC_METHOD_SCAN_NAME + METRIC_TIMER);
    timerMapping.put(BATCH_GET_TIMER, METRIC_METHOD_BATCH_GET_NAME + METRIC_TIMER);
    timerMapping.put(DELETE_TIMER, METRIC_METHOD_DELETE_NAME + METRIC_TIMER);
    timerMapping.put(BATCH_DELETE_TIMER, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_TIMER);
    timerMapping.put(CHECK_DELETE_TIMER, METRIC_METHOD_CHECK_DELETE_NAME + METRIC_TIMER);
    timerMapping.put(INDEX_GET_TIMER, METRIC_METHOD_INDEX_GET_NAME + METRIC_TIMER);
    timerMapping.put(MUTATE_TIMER, METRIC_METHOD_MUTATE_NAME + METRIC_TIMER);
    timerMapping.put(CHECK_MUTATE_TIMER, METRIC_METHOD_CHECK_AND_MUTATE_NAME + METRIC_TIMER );
    timerMapping.put(INCREMENT_TIMER, METRIC_METHOD_INCREMENT_NAME + METRIC_TIMER );
    timerMapping.put(CONNECTION_TIMER, METRIC_METHOD_CONNECTION_NAME + METRIC_TIMER);

    meterMapping.put(INDEX_PUT_INIT, METRIC_METHOD_INDEX_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(INDEX_PUT_COMPLETE, METRIC_METHOD_INDEX_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(INDEX_GET_INIT, METRIC_METHOD_INDEX_GET_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(INDEX_GET_COMPLETE, METRIC_METHOD_INDEX_GET_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(APPEND_INIT, METRIC_METHOD_APPEND_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(APPEND_COMPLETE, METRIC_METHOD_APPEND_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(PUT_INIT, METRIC_METHOD_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(PUT_COMPLETE, METRIC_METHOD_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(BATCH_PUT_INIT, METRIC_METHOD_BATCH_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(BATCH_PUT_COMPLETE, METRIC_METHOD_BATCH_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(BATCH_INIT, METRIC_METHOD_BATCH_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(BATCH_COMPLETE, METRIC_METHOD_BATCH_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(CAS_INIT, METRIC_METHOD_CAS_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(CAS_COMPLETE, METRIC_METHOD_CAS_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(CHECK_DELETE_INIT, METRIC_METHOD_CHECK_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(CHECK_DELETE_COMPLETE, METRIC_METHOD_CHECK_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(DELETE_INIT, METRIC_METHOD_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(DELETE_COMPLETE, METRIC_METHOD_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(BATCH_DELETE_INIT, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(BATCH_DELETE_COMPLETE, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(GET_INIT, METRIC_METHOD_GET_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(GET_COMPLETE, METRIC_METHOD_GET_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(SCAN_INIT, METRIC_METHOD_SCAN_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(SCAN_COMPLETE, METRIC_METHOD_SCAN_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(BATCH_GET_INIT, METRIC_METHOD_BATCH_GET_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(BATCH_GET_COMPLETE, METRIC_METHOD_BATCH_GET_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(MUTATE_INIT, METRIC_METHOD_MUTATE_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(MUTATE_COMPLETE, METRIC_METHOD_MUTATE_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(CHECK_MUTATE_INIT, METRIC_METHOD_CHECK_AND_MUTATE_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(CHECK_MUTATE_COMPLETE, METRIC_METHOD_CHECK_AND_MUTATE_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(INCREMENT_INIT, METRIC_METHOD_INCREMENT_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(INCREMENT_COMPLETE, METRIC_METHOD_INCREMENT_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    meterMapping.put(CONNECTION_INIT, METRIC_METHOD_CONNECTION_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(CONNECTION_COMPLETE, METRIC_METHOD_CONNECTION_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);

    errorMapping.put(BATCH_GEN_EXCEPTION, METRIC_METHOD_BATCH_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(BATCH_PUT_GEN_EXCEPTION, METRIC_METHOD_BATCH_PUT_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(CAS_GEN_EXCEPTION, METRIC_METHOD_CAS_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(CHECK_DELETE_GEN_EXCEPTION, METRIC_METHOD_CHECK_DELETE_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(DELETE_GEN_EXCEPTION, METRIC_METHOD_DELETE_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(BATCH_DELETE_GEN_EXCEPTION, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(GET_GEN_EXCEPTION, METRIC_METHOD_GET_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(SCAN_GEN_EXCEPTION, METRIC_METHOD_SCAN_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(INDEX_PUT_GEN_EXCEPTION, METRIC_METHOD_INDEX_PUT_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(APPEND_GEN_EXCEPTION, METRIC_METHOD_APPEND_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(PUT_GEN_EXCEPTION, METRIC_METHOD_PUT_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(INDEX_GET_GEN_EXCEPTION, METRIC_METHOD_INDEX_GET_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(BATCH_GET_GEN_EXCEPTION, METRIC_METHOD_BATCH_GET_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(MUTATE_GEN_EXCEPTION, METRIC_METHOD_MUTATE_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(CHECK_MUTATE_GEN_EXCEPTION, METRIC_METHOD_MUTATE_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(INCREMENT_GEN_EXCEPTION, METRIC_METHOD_INCREMENT_NAME + METRIC_EXCEPTIONS_TYPE);
    errorMapping.put(CONNECTION_GEN_EXCEPTION, METRIC_METHOD_CONNECTION_NAME + METRIC_EXCEPTIONS_TYPE);


    // Start metric on application start
    for (String key : meterMapping.keySet()) {
      incrementMetric(key, 0L, false);
    }

    for (String key : errorMapping.keySet()) {
      incrementMetric(errorMapping.get(key) + METRIC_STORE_EXCEPTIONS, 0L, true);
    }
    for (String key : timerMapping.keySet()) {
      getTimer(key).close();
    }

    for (String key : counterMapping.keySet()) {
      updateCounter(key, 0L);
    }
  }

  public void incrementMetric(String metricName) {
    this.publisher.markMeter(meterMapping.get(metricName));
  }

  public void incrementMetric(String metricName, long value, boolean isMetricName) {
    if (isMetricName) {
      this.publisher.markMeter(metricName, value);
    } else {
      this.publisher.markMeter(meterMapping.get(metricName), value);
    }
  }

  public void incrementMetric(String metricName, boolean flag) {
    this.publisher.markMeter(meterMapping.get(metricName), flag);
  }

  public void updateThreadCounter(long activeCount, long queueSize, long threadCount) {
    updateCounter(StoreClientMetricsPublisher.ACTIVE_THREAD_COUNT, activeCount);
    updateCounter(StoreClientMetricsPublisher.QUEUE_SIZE, queueSize);
    updateCounter(StoreClientMetricsPublisher.THREAD_COUNT, threadCount);
  }

  public void updateCounter(String metricName, long newValue) {
    Counter counter = this.publisher.getCounter(counterMapping.get(metricName));
    long diff = counter.getCount() - newValue;
    if (diff > 0) {
      counter.dec(Math.abs(diff));
    } else if (diff < 0) {
      counter.inc(Math.abs(diff));
    }
  }

  public Timer.Context getTimer(String metricName) {
    return this.publisher.getTimer(timerMapping.get(metricName)).time();
  }

  public void incrementErrorMetric(String metricName, Throwable error) {
    this.publisher.markMeter(errorMapping.get(metricName) + METRIC_STORE_EXCEPTIONS);
    this.publisher.markMeter(errorMapping.get(metricName) + error.getClass().getSimpleName());
  }

  public void incrementErrorMetric(String metricName, String errorName) {
    this.publisher.markMeter(errorMapping.get(metricName) + METRIC_STORE_EXCEPTIONS);
    this.publisher.markMeter(errorMapping.get(metricName) + errorName);
  }
}
