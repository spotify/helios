# Building Helios and Running Tests

1. Under the parent level helios source directory run `mvn clean install`. This will run the integration tests. To avoid running the tests, add the `-DskipTests` option. 
2. Note that the above build command will produce a generate directory under `helios-client/target/generated-sources/templated` which may need to be added as a source directory to your IDE manually.
3. In order to run the Helios integration tests, first run: `mvn -P build-images -P build-solo package -DskipTests=true -Dmaven.javadoc.skip=true -B -V -pl helios-services`.

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
