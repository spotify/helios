Since Helios 0.8.662, [helios-solo][] can also be launched directly from within
an integration test using a JUnit `@ClassRule`/`@Rule`.

This allows for integration tests that will automatically install (and clean
up) helios-solo for you without having to install `helios-solo` and remember to
start it before running your tests.  The only requirement is that the machine
running the integration test have Docker installed.

# How to use it

If you have a test that is already using [TemporaryJobs][], the test can run
through a self-contained helios-solo Docker container with a few small modifications.
Your test will create and run a Docker container that has a running Helios cluster
inside and tear it down when the tests finish.

Instantiate a `HeliosDeploymentResource`:

```java
private static final HeliosDeploymentResource soloResource =
    new HeliosDeploymentResource(HeliosSoloDeployment.fromEnv().build());
```

`HeliosDeploymentResource` wraps a `HeliosDeployment` instance which describes
where the Helios instance is installed. `HeliosDeploymentResource` provides a
client that talks to the `HeliosDeployment` and cleans it up after the test is
finished.

Then modify the `TemporaryJobs` declaration to use the client that points to
your `HeliosSoloDeployment`:

```java
private static final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
    .client(soloResource.client())
    .build();
```

Then use a `RuleChain` to guarantee that `HeliosSoloDeployment` and then `TemporaryJobs`
are created in that order and torn down in the reverse order. Otherwise, you might
have leftover containers from your tests.


```java
@ClassRule
public static final RuleChain chain = RuleChain
    .outerRule(soloResource)
    .around(temporaryJobs);
```

## @ClassRule vs @Rule
The `@ClassRule RuleChain` above will set up `HeliosSoloDeployment` once for all of the tests
in your `Test` class. To instead set up (and tear down) a
`HeliosSoloDeployment` for *each individual* test in the class, change the
`@ClassRule` to a `@Rule` and remove the `static` modifier. 

Since there is a bit of set up work in creating a `HeliosSoloDeployment`, this
will add a lot of overhead to each test. We recommend using
`HeliosSoloDeployment` as a `@ClassRule`.

[helios-solo]: ./helios_solo.md
[TemporaryJobs]: ./testing_framework.md

## Known Issues

### `DOCKER_HOST` refers to `localhost` or `127.0.0.1`

In order to deploy jobs to itself, the helios-solo container needs to talk to
the docker daemon running on the host. 

If your `DOCKER_HOST` environment variable points to a Unix socket, or refers
to an address that is not `localhost` or `127.0.0.1` then this should work fine
(in the Unix socket case, `HeliosSoloDeployment` adds arguments to the
helios-solo container to bind the unix socket file).

If `DOCKER_HOST` refers to `localhost`/`127.0.0.1` then this will not work as
that address within the container refers back to the container itself.

In this case, `HeliosSoloDeployment` will attempt to be smart and override your
`DOCKER_HOST` and use the unix socket endpoint instead. If this does not work
for you (perhaps because you use a non-default Unix socket address) then you
can workaround this by making sure your test is launched with a `DOCKER_HOST`
value that is not localhost.

### Docker for Mac: TemporaryJob without healthcheck is immediately "up"
When using Docker for Mac, if you deploy a TemporaryJob with a port mapping but
without any healthchecks, your test will run immediately after the container
for TemporaryJob has started, which may be before the service inside it has
fully initialized.

This is because Docker for Mac seems to accept connections on the mapped port
immediately, before the container starts to listen on the port.  Helios-testing
connects to the mapped port to judge when the container has finished starting
up.

To work around this, add a healthcheck to your TemporaryJob.

### Docker for Mac: address for TemporaryJob containers is returned as 127.0.0.1
HeliosSoloDeployment has [special checks when running under Docker for
Mac][hsd-dfm] in order to determine what IP address should be used for your
test to communicate with the container running your TemporaryJob, i.e. the
return value from `job.address("port name")`.

These checks are necessary so that the IP address is not reported as
`127.0.0.1` (since Docker for Mac is exposing ports on the local host). A
return value of `127.0.0.1` will not work if your test intends to pass the
return value of `job.address("port name")` into a second container to have one
TemporaryJob talk to another.

These checks rely on having [the hostname of the local host be resolved into a
non-localhost IP address][hostaddress].

If you are encountering a problem where the return value of `job.address("port
name")` is `127.0.0.1`, check to make sure that you are not mapping your
hostname to localhost in `/etc/hosts` (some [bugs for IntelliJ may have
workarounds that call for doing this][intellij-bug]).

[hsd-dfm]: https://github.com/spotify/helios/commit/4951b7a57144cfbf0b788005ea85530f99664d2c
[hostaddress]: https://docs.oracle.com/javase/8/docs/api/java/net/InetAddress.html#getHostAddress--
[intellij-bug]: http://stackoverflow.com/questions/30625785/intellij-freezes-for-about-30-seconds-before-debugging
