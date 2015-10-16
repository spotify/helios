# Authentication

This document details how to configure your Helios installation to authenticate
requests between the Helios CLI and the Helios masters/cluster.

The support for authentication in Helios is pluggable; the specific scheme used
by an installation can be controlled based on the arguments that Helios is
started with. New authentication schemes can be added to Helios via
implementing a handful of Java interfaces, packaging your plugin as a jar, and
supplying the path to that JAR in an argument when starting the Helios cluster.

Helios comes with support for the [crtauth protocol][], which is in use
internally at Spotify, but users of Helios are free to choose any other scheme.

# TODOs for this document

- link to how to setup HTTPS for Helios masters

## Authentication flow

*Note that since all authentication requires clients to send some sort of
credentials over the wire to the Helios masters, you should first configure
your Helios clusters to use HTTPS.*

When configured to do so, the Helios master will respond to any unauthenticated
request with a `HTTP 401 Unauthorized` status code in the response. The master
will set the `WWW-Authenticate` header in the response with the configured
scheme for the client to use.

When the client receives this response status code, it will look for a plugin
registered for the given scheme. The Helios CLI has support for the
[`crtauth`][] scheme built in, but additional schemes can be added to the CLI
by doing ... *TODO*.

If the client cannot find a plugin for the scheme, it will be unable to proceed.

After finding the plugin, the client activates it. The plugin is then
responsible for performing authentication - for example in the crtauth flow (as
explained below), it is the plugin's role to handle making the multiple HTTP
requests for the two-step challenge-response flow. A simpler authentication
scheme may just read a shared secret from a file on disk or an environment
variable and populate the `Authorization` header with that value.

The end result of the authentication process is that the Helios client will
have a token for the `Authorization` header of it's HTTP requests that is known
to work. Some schemes may have the concept of expiring tokens, but to maintain
support for all types of schemes within Helios, the server does not communicate
this information back to the client. 

The server may respond back to any request that contains an `Authorization`
header with a new `401 Unauthorized` response (perhaps because the token has
expired). The client will handle this by attempting to invoke the
authentication flow again.

## Supported authentication schemes

- [crtauth][]
- [HTTP Basic Authentication][http-basic]

## How to configure the Helios masters to require authentication

Authentication is enabled in the master by adding the `--auth-enabled` flag.
The name of the authentication scheme to activate must be supplied in the
`--auth-scheme SCHEME` flag.

Authentication can be restricted to only apply to a certain client
version and above, to allow for rolling out the authentication requirement
slowly across a Helios installation. This is controlled with the
`--auth-minimum-version VERSION` argument when starting the master (where
`VERSION` is a Helios version string like `0.8.500`). If not set,
authentication is applied to all clients and requests.

To use an authentication plugin that is not shipped with Helios itself (such as
a scheme you have authored), include the path to the JAR containing the plugin
with the `--auth-plugin` argument to the master.

## How to add support to Helios for additional schemes

### How plugins are loaded

Helios uses the `java.util.ServiceLoader` facility for loading
Authenticator instances from the classpath. The first Authenticator
instance found that matches the configured scheme name is used. If no
Authenticator instance is found, the master will fail to start up.

### Implementing a new plugin

A new plugin needs to implement three interfaces: 

- com.spotify.helios.auth.ServerAuthentication, which tells the server (and
  Jersey) how to translate HTTP requests into "credentials" and verify them.
- com.spotify.helios.auth.ClientAuthentication, which tells the CLI/client what
  to do when the server signals that it requires authentication.
- com.spotify.helios.auth.AuthenticationPlugin, which is the class actually
  loaded by ServiceLoader and simply provides access to the
  ServerAuthentication and ClientAuthentication above.

A plugin providing HTTP Basic Authentication is packaged with the
helios-authentication module as an example of how to implement a plugin. The
packaged crtauth plugin can also be used as reference.

#### ServerAuthentication

There is one required part to implement in ServerAuthentication and one optional part.

The required part of the interface is:

```java
InjectableProvider<Auth, Parameter> authProvider();
```

`InjectableProvider<Auth, Parameter>` tells Jersey what to do when it
encounters resource classes that have parameters annotated with `@Auth`. The
main thing implementations of this class have to do (through the Jersey
`Injectable` they return) is to translate a HTTP request, represented by the
`com.sun.jersey.api.core.HttpContext` class, into an instance of `HeliosUser`.

The typical flow for implementations will be to examine the HTTP headers of the
request, and if they present and well-formed, validate the credentials into a
user/principal object (`HeliosUser`).

The recommended way to structure this, following the examples of
`io.dropwizard.auth.basic.BasicAuthProvider` and Helios' `CrtAuthProvider`, is
to use the Dropwizard concept of [`Authenticator`s][dw-authenticator], which
provides a useful definition for a component that takes credentials and
optionally turns them into a principal.

##### Adding additional Jersey controllers to Helios
The optional part of the ServerAuthentication interface is

```java
void registerAdditionalJerseyComponents(JerseyEnvironment env);
```

This method can be implemented to add extra components to the
JerseyEnvironment. In the case of crtauth, it adds another resource to provide
the two-step authentication flow of crtauth. If no new HTTP endpoints need to
be added (as in the case of the HTTP Basic implementation), the implementation
of the method can just be empty.

### ClientAuthentication

TODO



[crtauth]: https://github.com/spotify/crtauth-java
[crtauth-protocol]: https://github.com/spotify/crtauth/blob/master/PROTOCOL.md
[dw-authenticator]: http://dropwizard.github.io/dropwizard/0.7.1/docs/manual/auth.html
