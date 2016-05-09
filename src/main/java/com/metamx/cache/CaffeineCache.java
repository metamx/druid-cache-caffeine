/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class CaffeineCache implements io.druid.client.cache.Cache
{
  private static final Logger log = new Logger(CaffeineCache.class);
  private static final int FIXED_COST = 8; // Minimum cost in "weight" per entry;
  private final Cache<NamedKey, byte[]> cache;
  private final AtomicReference<CacheStats> priorStats = new AtomicReference<>(null);


  public static CaffeineCache create(final CaffeineCacheConfig config)
  {
    return create(config, config.getExecutor());
  }

  // Used in testing
  public static CaffeineCache create(final CaffeineCacheConfig config, @Nullable final Executor executor)
  {
    Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats();
    if (config.getExpiration() >= 0) {
      builder
          .expireAfterAccess(config.getExpiration(), TimeUnit.MILLISECONDS);
    }
    if (config.getMaxSize() >= 0) {
      builder
          .maximumWeight(config.getMaxSize())
          .weigher((NamedKey key, byte[] value) -> value.length
                                                   + key.key.length
                                                   + key.namespace.length() * Chars.BYTES
                                                   + FIXED_COST);
    }
    if (executor != null) {
      builder.executor(executor);
    }
    return new CaffeineCache(builder.build());
  }

  public CaffeineCache(final Cache<NamedKey, byte[]> cache)
  {
    this.cache = cache;
  }

  @Override
  public byte[] get(NamedKey key)
  {
    return deserialize(cache.getIfPresent(key));
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    cache.put(key, serialize(value));
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    // The assumption here is that every value is accessed at least once. Materializing here ensures deserialize is only
    // called *once* per value.
    return ImmutableMap.copyOf(Maps.transformValues(cache.getAllPresent(keys), this::deserialize));
  }

  // This is completely racy with put. Any values missed should be evicted later anyways. So no worries.
  @Override
  public void close(String namespace)
  {
    /***** Let LRU take care of cache invalidation
     final String keyPrefix = computeNamespaceHash(namespace) + ":";
     for (String key : cache.asMap().keySet()) {
     if (key.startsWith(keyPrefix)) {
     cache.invalidate(key);
     }
     }
     */
  }

  @Override
  public io.druid.client.cache.CacheStats getStats()
  {
    final com.github.benmanes.caffeine.cache.stats.CacheStats stats = cache.stats();
    final long size = cache
        .policy().eviction()
        .map(eviction -> eviction.isWeighted() ? eviction.weightedSize() : OptionalLong.empty())
        .orElse(OptionalLong.empty()).orElse(-1);
    return new io.druid.client.cache.CacheStats(
        stats.hitCount(),
        stats.missCount(),
        cache.estimatedSize(),
        size,
        stats.evictionCount(),
        0,
        stats.loadFailureCount()
    );
  }

  @Override
  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    final CacheStats oldStats = priorStats.get();
    final CacheStats newStats = cache.stats();
    final CacheStats deltaStats;
    if (oldStats == null) {
      deltaStats = newStats;
    } else {
      deltaStats = newStats.minus(oldStats);
    }
    final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    emitter.emit(builder.build("query/cache/caffeine/delta/requests", deltaStats.requestCount()));
    emitter.emit(builder.build("query/cache/caffeine/total/requests", newStats.requestCount()));
    emitter.emit(builder.build("query/cache/caffeine/delta/loadTime", deltaStats.totalLoadTime()));
    emitter.emit(builder.build("query/cache/caffeine/total/loadTime", newStats.totalLoadTime()));
    emitter.emit(builder.build("query/cache/caffeine/delta/evictionBytes", deltaStats.evictionWeight()));
    emitter.emit(builder.build("query/cache/caffeine/total/evictionBytes", newStats.evictionWeight()));
    if (!priorStats.compareAndSet(oldStats, newStats)) {
      // ISE for stack trace
      log.warn(
          new IllegalStateException("Multiple monitors"),
          "Multiple monitors on the same cache causing race conditions and unreliable stats reporting"
      );
    }
  }

  Cache<NamedKey, byte[]> getCache()
  {
    return cache;
  }

  private final LZ4Factory factory = LZ4Factory.fastestInstance();
  private final LZ4FastDecompressor decompressor = factory.fastDecompressor();
  private final LZ4Compressor compressor = factory.fastCompressor();

  private byte[] deserialize(byte[] bytes)
  {
    if (bytes == null) {
      return null;
    }
    final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
    final byte[] out = new byte[decompressedLen];
    decompressor.decompress(bytes, Ints.BYTES, out, 0, out.length);
    return out;
  }

  private byte[] serialize(byte[] value)
  {
    final int len = compressor.maxCompressedLength(value.length);
    final byte[] out = new byte[len];
    final int compressedSize = compressor.compress(value, 0, value.length, out, 0);
    return ByteBuffer.allocate(compressedSize + Ints.BYTES)
                     .putInt(value.length)
                     .put(out, 0, compressedSize)
                     .array();
  }
}
