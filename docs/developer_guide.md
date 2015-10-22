# Building Helios

TODO

Use `bin/helios` and `bin/helios-master` to start the Helios CLI and server
(respectively) from the locally-built project.

## Running helios-system-tests locally

If you are running helios-system-tests locally and are wondering where the
slf4j/logback output is when the tests are being run, SystemTestBase will by
default configure logback to be extra verbose and [to write
logs][LoggingTestWatcher] to `/tmp/helios-test/log/`.

This behavior can be disabled by disabling the `logToFile` system property when
maven-surefire-plugin is invoked.

[LoggingTestWatcher]: ../helios-system-tests/src/main/java/com/spotify/helios/system/LoggingTestWatcher.java
