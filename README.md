Druid Caffeine Cache
--------------------

**THIS IS MOVING TO DRUID.IO AS PER** https://github.com/druid-io/druid/pull/3028

A local cache implementation for Druid based on Caffeine. Requires a JRE with a fix for https://bugs.openjdk.java.net/browse/JDK-8078490

# Versioning

The versioning works like this: `druid_version.patch_set`. Such that `druid_version` is the version of druid the extension was compiled against, and `patch_set` is the "version" of the extension.

# How to use
The maven artifact coordinate for this extension is `com.metamx.cache:druid-caffeine-cache:0.8.3.2` (or whatever the latest tag version is).

## Druid 0.8.3

For Druid 0.8.3, the extension can be included by adding `com.metamx.cache:druid-caffeine-cache:0.8.3.2` to your extension coordinates.

## Druid 0.9.0

For Druid 0.9.0 and later, the extension can be included by pulling the jars using the `pull-deps` tool, and including the extension directory in the extension load list.

The `0.8.3.2` release works with Druid 0.9.0

### Jars

For the sake of sanity if you are manaully making an extension directory for 0.9.0, the `0.8.3.2` release requires the following jars: 

* caffeine-2.2.6.jar
* druid-caffeine-cache-0.8.3.2.jar

# Configuration
Below are the configuration options known to this module:

|`runtime.properties`|Description|Default|
|--------------------|-----------|-------|
|`druid.cache.sizeInBytes`|The maximum size of the cache in bytes on heap.|None (unlimited)|
|`druid.cache.expireAfter`|The time (in ms) after an access for which a cache entry may be expired|None (no time limit)|
|`druid.cache.cacheExecutorFactory`|The executor factory to use for Caffeine maintenance|ForkJoinPool common pool|
|`druid.cache.evictOnClose`|If a close of a namespace (ex: removing a segment from a node) should cause an eager eviction of associated cache values|`false`|

# Metrics
In addition to the normal cache metrics, the caffeine cache implementation also reports the following in both `total` and `delta`

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/caffeine/*/requests`|Count of hits or misses|hit + miss|
|`query/cache/caffeine/*/loadTime`|Length of time caffeine spends loading new values (unused feature)|0|
|`query/cache/caffeine/*/evictionBytes`|Size in bytes that have been evicted from the cache|Varies, should tune cache `sizeInBytes` so that `sizeInBytes`/`evictionBytes` is approximately the rate of cache churn you desire|
