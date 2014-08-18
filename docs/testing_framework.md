Reviewed by [rculbertson](https://github.com/rculbertson) on 2014-08-18

***

The Helios testing framework lets you deploy multiple services as docker containers, and run automated tests against them. The tests can be run locally or as part of an automated build. You can use the containers available in the Docker Registry, as well as containers built on your local machine.

The framework integrates with standard JUnit tests, and provides a JUnit rule called `TemporaryJobs`. This rule is the starting point for using the framework, and is used to define helios jobs, deploy them, and automatically undeploy them when the tests have completed.

# Instructions

1. Make sure running `mvn package` builds your service into a docker container. You can do this with the [docker-maven-plugin](https://github.com/spotify/docker-maven-plugin).

2. Add helios-testing to your pom.xml in the <dependencies> element. Use the latest version if a newer one is available.
    ```
    <dependency>
        <groupId>com.spotify</groupId>
        <artifactId>helios-testing</artifactId>
        <version>0.8.1</version>
        <scope>test</scope>
    </dependency>
    ```
3. Add the failsafe plugin to your pom.xml within the <build><plugins> element.
    ```
    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-failsafe-plugin</artifactId>
        <version>2.17</version>
        <executions>
            <execution>
                <goals>
                    <goal>integration-test</goal>
                    <goal>verify</goal>
                </goals>
            </execution>
        </executions>
    </plugin>
    ```

4. Create an integration test for your service following the examples below. The filename must end in “IT.java” so that maven's failsafe plugin will recognize it as an integration test.

This basic example shows how to deploy a single service called Wiggum.
```java
package com.spotify.wiggum;

import com.spotify.helios.testing.TemporaryJob;
import com.spotify.helios.testing.TemporaryJobs;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.message.StatusCode;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class ServiceIT {

  @Rule
  public TemporaryJobs temporaryJobs = TemporaryJobs.create();

  private Client client;

  @Before
  public void setup() {
    // Deploy the wiggum image created during the last build.The wiggum container will be listening
    // on a dynamically allocated port. 4229 is the port wiggum listens on in the container.
    temporaryJob = temporaryJobs.job()
        .imageFromBuild()
        .port("wiggum", 4229)
        .deploy();
     
    // create a hermes client using the host and dynamically allocated port of the job
    client = new Client("tcp://" + temporaryJob.address("wiggum"));
  }

  @Test
  public void testPing() throws Exception {    
    final Message message = client.ping().get(10, SECONDS);
    assertEquals("ping failed", StatusCode.OK, message.getStatusCode());
  }
```

This example deploys cassandra, memcached, and a service called foobar. Helios will register 
cassandra and memcached with a service discovery mechanism using the service registration plugin.
Foobar will then lookup each service via that mechanism, just as it would in production. 

```java
package com.spotify.foobar;

import com.google.common.net.HostAndPort;

import com.spotify.foobar.store.CassandraClient;
import com.spotify.helios.testing.TemporaryJob;
import com.spotify.helios.testing.TemporaryJobs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SystemIT {

  @Rule
  public final TemporaryJobs temporaryJobs = TemporaryJobs.create();

  private Client client;

  @Before
  public void setup() throws Exception {

    final TemporaryJob cassandra = temporaryJobs.job()
        .image("spotify/cassandra")
        .port("cassandra", 9160)
        .registration("cassandra", "tcp", "cassandra")
        .deploy();

    final TemporaryJob memcached = temporaryJobs.job()
        .image("ehazlett/memcached")
        .env("MEMORY", "16")
        .port("memcached", 11211)
        .registration("memcached", "tcp", "memcached")
        .deploy();

    final TemporaryJob foobar = temporaryJobs.job()
        .imageFromBuild()
        .env("CASSANDRA_MAX_CONNS_PER_HOST", String.valueOf(1))
        .port("foobar", 9600)
        .deploy();

    client = new Client("tcp://" + foobar.address("foobar"));

    final HostAndPort address = cassandra.addresses("cassandra").get(0);
    final CassandraClient cassandraClient = new CassandraClient();
    cassandraClient.connect(address.getHostText(), address.getPort());
    cassandraClient.populateTestData();
  }

  @Test
  public void test() throws Exception {
    ...
  }
}
```

# Environment Configuration

There are 3 environment variables you can use to configure the test to run in different environments.
 
  * `HELIOS_DOMAIN` - the domain where the helios master can be reached.
  * `HELIOS_ENDPOINTS` - the specific endpoint(s) to connect to. When set, this overrides HELIOS_DOMAIN.
  * `HELIOS_HOST_FILTER` - regular expression or FQDN of the helios host you want to deploy to. If more
    than one host matches the regex, one will be selected randomly.
   
   If neither `HELIOS_DOMAIN` nor `HELIOS_ENDPOINTS` is set, TemporaryJobs will connect to `tcp://localhost:5801` and set `HELIOS_HOST_FILTER` to `.+`. `HELIOS_HOST_FILTER` must be set if either `HELIOS_DOMAIN` or `HELIOS_ENDPOINTS` is set. 
  
# Running locally

1. Make sure Helios and Docker are running locally.

2. If Helios is exposed on `localhost:5801`, there is no need to set any environment variables. 
   By default it will connect to `localhost:5801` and will set HELIOS_HOST_FILTER to `.+`.
    
    If Helios is not exposed on `localhost:5801`, you will need to set these variables. This would be the
    case if you are running helios in a VM and ports must be reached through the VM's IP address.

    `HELIOS_ENDPOINTS=tcp://<host>:5801`
    
    `HELIOS_HOST_FILTER=.+`

3. Run `mvn clean verify`. This will build the container, and run the integration test. You can also run the integration test in IntelliJ.

# Running from a build server

1. In your build configuration

    Set either `HELIOS_DOMAIN` or `HELIOS_ENDPOINTS` to connect to Helios. `HELIOS_DOMAIN` is preferred.
    
    Set `HELIOS_HOST_FILTER` to a regex or FQDN of the host you want to deploy to.

2. Make sure your build runs `mvn clean verify` which will build the docker image and run the integration test.

# Job Cleanup

When a test completes, the TemporaryJobs rule will undeploy and delete all jobs it created during the test. If the test process is killed before the test completes, the jobs will be left running. Helios handles this in two ways.

1. Each time TemporaryJobs is created, it will attempt to remove any jobs leftover from previous runs.

2. TemporaryJobs sets a time-to-live on each job it creates. This defaults to 30 minutes, but can be overridden if needed.


