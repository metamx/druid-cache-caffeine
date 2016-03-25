Druid Caffeine Cache
--------------------

A local cache implementation for Druid based on Caffeine

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

* caffeine-2.1.0.jar
* druid-caffeine-cache-0.8.3.2.jar
