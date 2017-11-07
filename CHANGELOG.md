## 3.1.3
  - Update gemspec summary

## 3.1.2
  - Fix some documentation issues

## 3.1.0
 - Add support for bulk inserts to improve performance.

## 3.0.1
 - Docs: Fix doc generation issue by removing extraneous comments and adding a short description of the plugin

## 3.0.0
 - Breaking: Updated plugin to use new Java Event APIs
 - relax logstash-core-plugin-api constrains
 - update .travis.yml

## 2.0.5
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 2.0.4
  - New dependency requirements for logstash-core for the 5.0 release

## 2.0.3
 - Patch Timestamp and BigDecimal with to_bson method and register with BSON.

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

## 1.0.0
 - Fixes the plugin to be in the 2.0 series
 - Add integration and unit test to the project
 - Adapt the codebase to be 2.0 compatible
 - Make the internal logger in mongo to report to LS logger

## 0.2.0
 - Add basic registration test to the project
