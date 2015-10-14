# Authentication

This document details how to configure your Helios installation to authenticate
requests between the Helios CLI and the Helios masters/cluster.

# TODOs for this document

- configuration needed to setup master to require authentication
  - make sure authentication can be enabled based on client version header, to
    apply to versions >= some value and/or completely
- add notion of schemes
- link to how to setup https for helios masters

## Authentication flow

*Note that since all authentication requires clients to send some sort of
credentials over the wire to the Helios masters, you should first configure
your Helios clusters to use HTTPS*

When configured to do so, the Helios master(s) will respond to any
unauthenticated request with a `HTTP 401 Unauthorized` status code in the
response. The master will set the `WWW-Authenticate` header in the response
with the configured scheme for the client to use.

When the client receives this response status code, it will look for a plugin
registered for the given scheme. The Helios CLI will have support for the
[`crtauth`][] scheme built in, but additional schemes can be added to the CLI
by doing ... *TODO*.

If the client cannot find a plugin for the scheme, it will be unable to proceed.

After finding the plugin, the client activates it. The plugin is then
responsible for performing authentication - for example in the crtauth flow (as
explained below), it is the plugin's role to handle making the multiple HTTP
requests for the two-step challenge-response flow.

The end result of authenticating is that the Helios server gives the client an
access token which should be included in the `Authorization` header of any
subsequent HTTP requests.

Access tokens have an associated expiration time, although the time at which
the token expires is not communicated to the client. When the client makes a
request to a protected resource on the service with an expired access token,
the server will again respond with `401 Unauthorized` to signal that the client
needs to perform the authentication flow again.

### crtauth flow

To begin the crtauth flow, the client sends a request to `/_auth` on one of the
masters to request a challenge. The client's request includes the username of
the current user (`System.getProperty("user.name")` in Java, but this should be
overridable via the CLI). The server will response with a challenge.

The client signs the challenge using the user's configured SSH key and sends it
to the server in a second request. 

The server verifies that the signed challenge is valid for the given user. If
valid, the server sends back an access token that the client is to use in all
subsequent requests to the server. 


## Supported authentication schemes

- [crtauth][]


## How to configure the Helios masters to require authentication

*TODO - document CLI args when starting the master, and how to restrict
authentication to only certain client versions*

## How to add support to Helios for additional schemes

Pieces for implementing support for a new scheme:

server-side:
```java
interface Authenticator {
    /** 
     * Given a token supplied in the "Authorization" header of the HTTP
     * request to Helios master, check if the token is valid.
     * An expired token should be treated like an invalid token by
     * returning false.
     */
    boolean verifyToken(String username, String token)

    /** 
     * Allows the authentication scheme to register additional HTTP endpoints
     * in Helios in the form of Jersey resource classes. This allows the scheme
     * to customize the flow for performing the authentication handshake, for
     * example to handle the two-step request-response cycle for crtauth.
     * 
     * If the scheme requires no custom HTTP endpoints (for instance, if just
     * validating a pre-shared secret token), return Optional.absent().
     */
    Optional<AuthenticationFlowEndpoint> authenticationFlowEndpoint();
}

interface AuthenticationFlowEndpoint {
    /** 
     * Return an Object (i.e. Resource class instance) to be registered with
     * Jersey at Helios master-startup which provides the HTTP endpoints needed
     * for this scheme's authentication handshake.
     */
    Object createJerseyResource(com.spotify.helios.master.MasterConfig config);
}
```

*TODO - what classes to implement*


[crtauth]: https://github.com/spotify/crtauth
