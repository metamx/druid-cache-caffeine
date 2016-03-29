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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.cache.Cache;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CaffeineCache implements Cache
{
  private static final Logger log = new Logger(CaffeineCache.class);
  private final LoadingCache<NamedKey, byte[]> cache;
  private final AtomicReference<CacheStats> priorStats = new AtomicReference<>(null);
  private final Cache delegateCache;

  public static CaffeineCache create(final CaffeineCacheConfig config)
  {
    Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats();
    if (config.getExpiration() >= 0) {
      builder = builder
          .expireAfterAccess(config.getExpiration(), TimeUnit.MILLISECONDS);
    }
    if (config.getMaxSize() >= 0) {
      builder = builder.maximumWeight(config.getMaxSize());
      builder.weigher(new Weigher<NamedKey, byte[]>()
      {
        @Override
        public int weigh(@Nonnull NamedKey key, @Nonnull byte[] value)
        {
          return value.length;
        }
      });
    }

    final Cache delegateCache = config.getDelegateCache();
    final CacheLoader<NamedKey, byte[]> cacheLoader;
    if (delegateCache != null) {
      cacheLoader = new CacheLoader<NamedKey, byte[]>()
      {
        @Override
        public byte[] load(@Nonnull NamedKey key) throws Exception
        {
          return delegateCache.get(key);
        }

        @Override
        public Map<NamedKey, byte[]> loadAll(@Nonnull Iterable<? extends NamedKey> keys) throws Exception
        {
          final Iterable<NamedKey> keys0 = (Iterable<NamedKey>) keys;
          return Maps.transformValues(delegateCache.getBulk(keys0), CaffeineCache::serialize);
        }
      };
    } else {
      cacheLoader = new CacheLoader<NamedKey, byte[]>()
      {
        @Override
        public byte[] load(@Nonnull NamedKey key) throws Exception
        {
          return null;
        }

        @Override
        public Map<NamedKey, byte[]> loadAll(@Nonnull Iterable<? extends NamedKey> keys) throws Exception
        {
          return ImmutableMap.of();
        }
      };
    }
    return new CaffeineCache(builder.build(cacheLoader), delegateCache);
  }

  public CaffeineCache(final LoadingCache<NamedKey, byte[]> cache, final Cache delegateCache)
  {
    this.cache = cache;
    this.delegateCache = delegateCache;
  }

  @Override
  public byte[] get(NamedKey key)
  {
    return deserialize(cache.get(key));
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    cache.put(key, serialize(value));
    if (delegateCache != null) {
      delegateCache.put(key, value);
    }
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    return Maps.transformValues(cache.getAllPresent(keys), CaffeineCache::deserialize);
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
    if (delegateCache != null) {
      delegateCache.close(namespace);
    }
  }

  @Override
  public io.druid.client.cache.CacheStats getStats()
  {
    final com.github.benmanes.caffeine.cache.stats.CacheStats stats = cache.stats();
    return new io.druid.client.cache.CacheStats(
        stats.hitCount(),
        stats.missCount(),
        stats.loadSuccessCount() - stats.evictionCount(),
        cache.estimatedSize(),
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
    if (!priorStats.compareAndSet(oldStats, newStats)) {
      // ISE for stack trace
      log.warn(
          new IllegalStateException("Multiple monitors"),
          "Multiple monitors on the same cache causing race conditions and unreliable stats reporting"
      );
    }

    if (delegateCache != null) {
      delegateCache.doMonitor(emitter);
    }
  }

  private static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();

  private static byte[] deserialize(byte[] bytes)
  {
    if (bytes == null) {
      return null;
    }
    final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
    final byte[] out = new byte[decompressedLen];
    final int bytesRead = FACTORY.fastDecompressor().decompress(bytes, Ints.BYTES, out, 0, out.length);
    if (bytesRead != bytes.length - Ints.BYTES) {
      if (log.isDebugEnabled()) {
        log.debug("Bytes read [%s] does not equal expected bytes read [%s]", bytesRead, bytes.length - Ints.BYTES);
      }
    }
    return out;
  }

  private static byte[] serialize(byte[] value)
  {
    final LZ4Compressor compressor = FACTORY.fastCompressor();
    final int len = compressor.maxCompressedLength(value.length);
    final byte[] out = new byte[len];
    final int compressedSize = compressor.compress(value, 0, value.length, out, 0);
    return ByteBuffer.allocate(compressedSize + Ints.BYTES)
                     .putInt(value.length)
                     .put(out, 0, compressedSize)
                     .array();
  }
}
