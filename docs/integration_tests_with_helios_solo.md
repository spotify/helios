Since Helios 0.8.662, [helios-solo][] can also be launched directly from within
an integration test using a JUnit `@ClassRule`/`@Rule`.

This allows for integration tests that will automatically install (and clean
up) helios-solo for you without having to install `helios-solo` and remember to
start it before running your tests.  The only requirement is that the machine
running the integration test have Docker installed.

# How to use it

If you have a test that is already using [TemporaryJobs][], the test can run
within a self-contained helios-solo instance with a few small modifications.

Add a `@ClassRule` that for helios-solo to install itself on your docker instance:

```java
@ClassRule
public static HeliosDeploymentResource soloResource =
    new HeliosDeploymentResource(HeliosSoloDeployment.fromEnv().build());
```

`HeliosDeploymentResource` wraps a `HeliosDeployment` instance which describes
where the Helios instance is installed. `HeliosDeploymentResource` provides a
client that talks to the `HeliosDeployment` and cleans it up after the test is
finished.

Then modify the `TemporaryJobs` declaration to use the client that points to
your `HeliosSoloDeployment`:

```java
@Rule
public TemporaryJobs temporaryJobs = TemporaryJobs.builder()
    .client(soloResource.client())
    .build();
```

## @ClassRule vs @Rule
The example above will set up `HeliosSoloDeployment` once for all of the tests
in your `Test` class. To instead set up (and tear down) a
`HeliosSoloDeployment` for *each individual* test in the class, change the
`@ClassRule` to a `@Rule` and remove the `static` modifier. 

Since there is a bit of set up work in creating a `HeliosSoloDeployment`, this
will add a lot of overhead to each test. We recommend using
`HeliosSoloDeployment` as a `@ClassRule`.

[helios-solo]: ./helios_solo.md
[TemporaryJobs]: ./testing_framework.md

## Issues if `DOCKER_HOST` refers to `localhost` or `127.0.0.1`

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
