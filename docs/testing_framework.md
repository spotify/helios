Reviewed by [rculbertson](https://github.com/rculbertson) on 2014-08-19

***

The Helios testing framework lets you deploy multiple services as docker containers, and run automated tests against them. The tests can be run locally or as part of an automated build. You can use the containers available in the Docker Registry, as well as containers built on your local machine.

The framework integrates with standard JUnit tests, and provides a JUnit rule called `TemporaryJobs`. This rule is the starting point for using the framework, and is used to define helios jobs, deploy them, and automatically undeploy them when the tests have completed.

* [Instructions](#instructions)
* [Environment Configuration](#environment-configuration)
* [Configuration By File](#configuration-by-file)
* [Job Cleanup](#job-cleanup)

# Instructions

1. Make sure running `mvn package` builds your service into a docker container. You can do this with the [docker-maven-plugin](https://github.com/spotify/docker-maven-plugin), by binding the build goal to the package lifecycle phase. When we run `mvn verify` we'll want to build the image first, and binding to the package phase lets us do that.  

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

5. Run `mvn verify`

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

    final HostAndPort address = cassandra.address("cassandra");
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

Note that cassandra and memcached register themselves using Helios's [service registration mechanism](service_registration.md).
Foobar can then discover how to reach them at runtime. Another option would be to pass Foobar their
endpoints via environment variables like this.

    final TemporaryJob foobar = temporaryJobs.job()
        .imageFromBuild()
        .env("MEMCACHED_ADDRESS", memcached.address("memcached")
        .env("CASSANDRA_ADDRESS", cassandra.addresses("cassandra")
        .env("CASSANDRA_MAX_CONNS_PER_HOST", String.valueOf(1))
        .env("CASSANDRA_MAX_CONNS_PER_HOST", String.valueOf(1))
        .port("foobar", 9600)
        .deploy();

# Environment Configuration

There are 3 environment variables you can use to configure the test to run in different environments.
 
  * `HELIOS_DOMAIN` - the domain where the helios master can be reached.
  * `HELIOS_ENDPOINTS` - the specific endpoint(s) to connect to. When set, this overrides HELIOS_DOMAIN.
  * `HELIOS_HOST_FILTER` - regular expression or FQDN of the helios host you want to deploy to. If more
    than one host matches the regex, one will be selected randomly.
   
   If neither `HELIOS_DOMAIN` nor `HELIOS_ENDPOINTS` is set, TemporaryJobs will connect to `tcp://localhost:5801` and set `HELIOS_HOST_FILTER` to `.+`. `HELIOS_HOST_FILTER` must be set if either `HELIOS_DOMAIN` or `HELIOS_ENDPOINTS` is set. 

# Configuration By File

It may be that for your site, there are common environment variables
or other settings which you want most/all `TemporaryJob` based tests
to have.  Rather than have to subclass the framework or hack things,
or whatnot, you can specify configuration profiles for these kinds of
things to save the boilerplate that would otherwise wind up in your
code.  The Helios Testing Framework uses the
[Typesafe Config Library](https://github.com/typesafehub/config) to
accomplish this. Within a profile, the following keys having meaning:

* `name`  - string -- same as the `TemporaryJobBuilder.name()` method.
* `version` - string -- same as the `TemporaryJobBuilder.version()` method.
* `image` - string -- same as the `TemporaryJobBuilder.image()` method.
* `command` - a list of strings -- same as the `TemporaryJobBuilder.command()` method
* `host` - string -- same as the `TemporaryJobBuilder.host()` method.
* `deploy` - string -- same as the `TemporaryJobBuilder.deploy()` method.
* `imageInfoFile` - string -- same as the `TemporaryJobBuilder.imageInfoFile()` method.
* `env` - map of string:string -- same as the `TemporaryJobBuilder.env()` method with entries from the map.
* `registrationDomain` - string -- same as the `TemporaryJobBuilder.registrationDomain()` method.
* `user` - string -- same as the `TemporaryJobsbuilder.user()` method.
* `hostFilter`  - string -- same as the `TemporaryJobsbuilder.hostFilter()` method.
* `domain`  - string -- same as the `TemporaryJobsbuilder.domain()` method.
* `endpoints`  - list of string -- same as the `TemporaryJobsbuilder.endpoints()` method.
* `jobDeployedMessageFormat` - string -- a string format for [Apache StrSubstitutor](https://commons.apache.org/proper/commons-lang/javadocs/api-2.6/org/apache/commons/lang/text/StrSubstitutor.html) to log a custom message when the job has started -- available substitutions are [`host`, `name`, `version`, `hash`, `containerId`, `job`] - useful to log a link where container logs might be found.
* `hostPickingStrategy` - string -- one of
    * `random` (the default) - randomly pick a host for each temporary job.
    * `onerandom` - randomly pick a host and deploy all temporary jobs there
    * `deterministic` - seed the random number generator so it
       "randomly" picks the same hosts across test runs that it
       did in previous runs.  You must specify `hostPickingStrategyKey`.
    * `onedetermistic` - seed the random number generator so it
       "randomly" picks the same host on which it deploys all
       jobs across test runs. You must specify `hostPickingStrategyKey`.
* `hostPickingStrategyKey` - string - a string used to seed the random number
  generator.  It can be basically anything, but it's best for it to not
  be empty.

There's also the `prefix` config variable, which is useless to set,
but useful to reference from some of the above variables if you want
the randomized prefix generated by the framework for other purposes.

Instead of the normal `application.conf` and `reference.conf` files that
Typesafe uses, the testing framework uses `helios.conf` and `helios-base.conf`
 respectively for these roles.

A sample `helios.conf` file (or `helios-base.conf`)
you could put in your project might be:

```
helios.testing.profile : foo
helios.testing.profiles {
  foo : {
    env : {
      ## Do beware that the ${prefix} is *not* enclosed in double quotes.
      MYJOBPREFIX : "something before ["${prefix}"] after"
    }
    hostFilter : "*testagent*"
  }
  bar : {
    env : {
      MYJOBPREFIX : "hahahah! no prefix for you!"
    }
  }
}
```

You can also consult the [`helios-base.conf`](../helios-testing/src/test/resources/helios-base.conf)
and [`helios.conf`](../helios-testing/src/test/resources/helios.conf) files
for an example.

**N.B.** If you specify the profile in code via the call to
`TemporaryJobs.builder(profile)`, it will override the setting in the
file.  Should this be the case?  How else to do something like what I
want for this?!

Also, any settings on the builders in code will override what's in the
file.

If the profile specified either to the builder directly, or by the
`profile` setting does not exist, you'll get a failure.  If there is
no profile set, this is still ok.  Since we're using the Typesafe
Library, you can then make the default profile settable by java
property or environment variable, which can then be configured via
Maven (if you're using it) profiles.

# Job Cleanup

When a test completes, the TemporaryJobs rule will undeploy and delete all jobs it created during the test. If the test process is killed before the test completes, the jobs will be left running. Helios handles this in two ways.

1. Each time TemporaryJobs is created, it will attempt to remove any jobs leftover from previous runs.

2. TemporaryJobs sets a time-to-live on each job it creates. This defaults to 30 minutes, but can be overridden if needed.


