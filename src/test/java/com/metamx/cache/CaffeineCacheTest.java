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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheProvider;
import io.druid.client.cache.CacheStats;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.Initialization;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CaffeineCacheTest
{
  private static final byte[] HI = "hiiiiiiiiiiiiiiiiiii".getBytes();
  private static final byte[] HO = "hooooooooooooooooooo".getBytes();

  private CaffeineCache cache;
  private final CaffeineCacheConfig cacheConfig = new CaffeineCacheConfig();

  @Before
  public void setUp() throws Exception
  {
    cache = CaffeineCache.create(cacheConfig);
  }

  @Test
  public void testBasicInjection() throws Exception
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig();
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);

                binder.bind(CaffeineCacheConfig.class).toInstance(config);
                binder.bind(Cache.class).toProvider(CaffeineCacheProviderWithConfig.class).in(ManageLifecycle.class);
              }
            }
        )
    );
    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    try {
      Cache cache = injector.getInstance(Cache.class);
      Assert.assertEquals(CaffeineCache.class, cache.getClass());
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testSimpleInjection()
  {
    final String uuid = UUID.randomUUID().toString();
    System.setProperty(uuid + ".type", "caffeine");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);

                binder.bind(Cache.class).toProvider(CacheProvider.class);
                JsonConfigProvider.bind(binder, uuid, CacheProvider.class);
              }
            }
        )
    );
    final CacheProvider cacheProvider = injector.getInstance(CacheProvider.class);
    Assert.assertNotNull(cacheProvider);
    Assert.assertEquals(CaffeineCacheProvider.class, cacheProvider.getClass());
  }

  @Test
  public void testBaseOps() throws Exception
  {
    final Cache.NamedKey aKey = new Cache.NamedKey("a", HI);
    Assert.assertNull(cache.get(aKey));
    put(cache, aKey, 1);
    Assert.assertEquals(1, get(cache, aKey));

    /* Lazily deleted by LRU
    cache.close("a");
    Assert.assertNull(cache.get(aKey));
    */

    final Cache.NamedKey hiKey = new Cache.NamedKey("the", HI);
    final Cache.NamedKey hoKey = new Cache.NamedKey("the", HO);
    put(cache, hiKey, 10);
    put(cache, hoKey, 20);
    Assert.assertEquals(10, get(cache, hiKey));
    Assert.assertEquals(20, get(cache, hoKey));
    cache.close("the");
    /* Lazily deleted by LRU
    Assert.assertNull(cache.get(hiKey));
    Assert.assertNull(cache.get(hoKey));
    */

    Assert.assertNull(cache.get(new Cache.NamedKey("miss", HI)));

    final CacheStats stats = cache.getStats();
    Assert.assertEquals(3, stats.getNumHits());
    Assert.assertEquals(2, stats.getNumMisses());
  }

  @Test
  public void testGetBulk() throws Exception
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    Cache.NamedKey key1 = new Cache.NamedKey("the", HI);
    put(cache, key1, 2);

    Cache.NamedKey key2 = new Cache.NamedKey("the", HO);
    put(cache, key2, 10);

    Map<Cache.NamedKey, byte[]> result = cache.getBulk(
        Lists.newArrayList(
            key1,
            key2
        )
    );

    Assert.assertEquals(2, Ints.fromByteArray(result.get(key1)));
    Assert.assertEquals(10, Ints.fromByteArray(result.get(key2)));

    Cache.NamedKey missingKey = new Cache.NamedKey("missing", HI);
    result = cache.getBulk(Lists.newArrayList(missingKey));
    Assert.assertEquals(result.size(), 0);

    result = cache.getBulk(Lists.<Cache.NamedKey>newArrayList());
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testSizeEviction() throws InterruptedException
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getMaxSize()
      {
        return 40;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final CaffeineCache cache = CaffeineCache.create(config, Runnable::run);

    Assert.assertNull(cache.get(key1));
    Assert.assertNull(cache.get(key2));

    cache.put(key1, val1);
    Assert.assertArrayEquals(val1, cache.get(key1));
    Assert.assertNull(cache.get(key2));

    Assert.assertEquals(0, cache.getCache().stats().evictionWeight());

    Assert.assertArrayEquals(val1, cache.get(key1));
    Assert.assertNull(cache.get(key2));

    cache.put(key2, val2);
    Assert.assertNull(cache.get(key1));
    Assert.assertArrayEquals(val2, cache.get(key2));
    Assert.assertEquals(34, cache.getCache().stats().evictionWeight());
  }

  @Test
  public void testSizeCalculation()
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getMaxSize()
      {
        return 40;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final Cache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());
  }

  @Test
  public void testSizeCalculationAfterDelete()
  {
    final String namespace = "the";
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getMaxSize()
      {
        return 999999;
      }
      @Override
      public boolean isEvictOnClose()
      {
        return true;
      }

    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey(namespace, s1);
    final Cache.NamedKey key2 = new Cache.NamedKey(namespace, s2);
    final Cache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(2L, stats.getNumEntries());
    Assert.assertEquals(68L, stats.getSizeInBytes());

    cache.close(namespace);
    stats = cache.getStats();
    Assert.assertEquals(0, stats.getNumEntries());
    Assert.assertEquals(0, stats.getSizeInBytes());
  }


  @Test
  public void testSizeCalculationMore()
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getMaxSize()
      {
        return 400;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final Cache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(2L, stats.getNumEntries());
    Assert.assertEquals(68L, stats.getSizeInBytes());
  }

  @Test
  public void testSizeCalculationNoWeight()
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getMaxSize()
      {
        return -1;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final Cache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(-1L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(-1L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(2L, stats.getNumEntries());
    Assert.assertEquals(-1L, stats.getSizeInBytes());
  }

  @Test
  public void testFromProperties()
  {
    final String keyPrefix = "cache.config.prefix";
    final Properties properties = new Properties();
    properties.put(keyPrefix + ".expiration", "10");
    properties.put(keyPrefix + ".maxSize", "100");
    properties.put(keyPrefix + ".cacheExecutorFactory", "single_thread");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              JsonConfigProvider.bind(binder, keyPrefix, CaffeineCacheConfig.class);
            }
        )
    );
    final JsonConfigurator configurator = injector.getInstance(JsonConfigurator.class);
    final JsonConfigProvider<CaffeineCacheConfig> caffeineCacheConfigJsonConfigProvider = JsonConfigProvider.of(
        keyPrefix,
        CaffeineCacheConfig.class
    );
    caffeineCacheConfigJsonConfigProvider.inject(properties, configurator);
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get().get();
    Assert.assertEquals(10, config.getExpiration());
    Assert.assertEquals(100, config.getMaxSize());
    Assert.assertNotNull(config.createExecutor());
  }

  @Test
  public void testMixedCaseFromProperties()
  {
    final String keyPrefix = "cache.config.prefix";
    final Properties properties = new Properties();
    properties.put(keyPrefix + ".expiration", "10");
    properties.put(keyPrefix + ".maxSize", "100");
    properties.put(keyPrefix + ".cacheExecutorFactory", "CoMmON_FjP");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              JsonConfigProvider.bind(binder, keyPrefix, CaffeineCacheConfig.class);
            }
        )
    );
    final JsonConfigurator configurator = injector.getInstance(JsonConfigurator.class);
    final JsonConfigProvider<CaffeineCacheConfig> caffeineCacheConfigJsonConfigProvider = JsonConfigProvider.of(
        keyPrefix,
        CaffeineCacheConfig.class
    );
    caffeineCacheConfigJsonConfigProvider.inject(properties, configurator);
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get().get();
    Assert.assertEquals(10, config.getExpiration());
    Assert.assertEquals(100, config.getMaxSize());
    Assert.assertNull(config.createExecutor());
  }

  @Test
  public void testDefaultFromProperties()
  {
    final String keyPrefix = "cache.config.prefix";
    final Properties properties = new Properties();
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              JsonConfigProvider.bind(binder, keyPrefix, CaffeineCacheConfig.class);
            }
        )
    );
    final JsonConfigurator configurator = injector.getInstance(JsonConfigurator.class);
    final JsonConfigProvider<CaffeineCacheConfig> caffeineCacheConfigJsonConfigProvider = JsonConfigProvider.of(
        keyPrefix,
        CaffeineCacheConfig.class
    );
    caffeineCacheConfigJsonConfigProvider.inject(properties, configurator);
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get().get();
    Assert.assertEquals(-1, config.getExpiration());
    Assert.assertEquals(-1, config.getMaxSize());
    Assert.assertNull(config.createExecutor());
  }

  public int get(Cache cache, Cache.NamedKey key)
  {
    return Ints.fromByteArray(cache.get(key));
  }

  public void put(Cache cache, Cache.NamedKey key, Integer value)
  {
    cache.put(key, Ints.toByteArray(value));
  }
}

class CaffeineCacheProviderWithConfig extends CaffeineCacheProvider
{
  private final CaffeineCacheConfig config;

  @Inject
  public CaffeineCacheProviderWithConfig(CaffeineCacheConfig config)
  {
    this.config = config;
  }

  @Override
  public Cache get()
  {
    return CaffeineCache.create(config);
  }
}
