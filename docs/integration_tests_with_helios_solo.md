Since Helios 0.8.662, [helios-solo][] can also be run directly from within an
integration test using a JUnit `@ClassRule`/`@Rule`.

This allows integration tests to run against Helios on the local machine
executing the tests without having to have `helios-solo` already installed
and/or running.

In other words, integration tests can be written that will automatically
install (and clean up) helios-solo for you without having to install any extra
packages beforehand on your local machine / CI server / etc. The only
requirement is that the machine running the integration test have Docker
installed.

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

[helios-solo]: docs/helios_solo.md
[TemporaryJobs]: docs/testing_framework.md
