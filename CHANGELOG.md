## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

# 1.0.0
  - Fixes the plugin to be in the 2.0 series
  - Add integration and unit test to the project
  - Adapt the codebase to be 2.0 compatible
  - Make the internal logger in mongo to report to LS logger

# 0.2.0
  - Add basic registration test to the project
