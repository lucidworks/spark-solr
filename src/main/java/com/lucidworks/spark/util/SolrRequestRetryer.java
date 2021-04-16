package com.lucidworks.spark.util;

import com.google.common.collect.Maps;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.http.NoHttpResponseException;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;

public class SolrRequestRetryer {
    public static final long DEFAULT_MAX_DURATION_MS = 60000L;
    public static final long DEFAULT_MAX_DELAY_MS = 10000L;
    public static final long DEFAULT_MAX_BACKOFF_DELAY_MS = 2500L;
    public static Logger log = Logger.getLogger(SolrRequestRetryer.class);

    private static final Map<String, SolrRequestRetryer> RETRY_POLICY_MAP = Maps.newConcurrentMap();

    private final RetryPolicy<NamedList<Object>> retryPolicy;
    private boolean enabled = true;

    public static SolrRequestRetryer defaultInstance() {
        return instance(DEFAULT_MAX_BACKOFF_DELAY_MS, DEFAULT_MAX_DELAY_MS, DEFAULT_MAX_DURATION_MS);
    }

    public static SolrRequestRetryer instance(long backoffDelayMs, long maxDelayMs, long maxDurationMs) {
        return RETRY_POLICY_MAP.computeIfAbsent(String.format("%d-%d-%d", backoffDelayMs, maxDelayMs, maxDurationMs), key -> new SolrRequestRetryer(new RetryPolicy<NamedList<Object>>()
                .withBackoff(backoffDelayMs, maxDelayMs, ChronoUnit.MILLIS)
                .withMaxDuration(Duration.ofMillis(maxDurationMs))
                .handle(Arrays.asList(SolrServerException.class,
                        SocketTimeoutException.class,
                        SolrException.class,
                        ConnectException.class,
                        NoHttpResponseException.class,
                        SocketException.class))
                .onRetry(retry -> log.error("Experienced an error when doing a SolrRequest. Performing a retry: " + retry))));
    }

    public SolrRequestRetryer(RetryPolicy<NamedList<Object>> retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public NamedList<Object> request(SolrClient solrClient, SolrRequest request) {
        return Failsafe.with(retryPolicy).get(() -> solrClient.request(request));
    }
}
