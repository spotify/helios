***

Helios can be configured to make it easy to register your containers into your
service discovery (registration) system, as well as register the masters in your service
registration system as well, so they can be found by the command line interface.

# Currently Known Supported Discovery Services
* [SkyDNS](https://github.com/spotify/helios-skydns)
* [Consul](https://github.com/SVT/helios-consul)

Hopefully soon, this list will be less sad.

# Container Registration
In order for Helios to be able to register containers into your service registration system,
you have to tell it ports and names and things.

There are two parts to this.  First is the port mapping.  This is specified with the `-p` option
to `helios create`.  The port mapping is independent of
service registration itself, but the name that you put there will be referenced when you specify
the service registration information.

The registration information is specified via the `-r` option to `helios create`.  Quoting from its
help text:
```
  -r REGISTER, --register REGISTER
      Service discovery registration. Specify a service name, the port name and a protocol
      on the format service/protocol=port. E.g. -r website/tcp=http will register the port named
      http with the protocol tcp. Protocol is optional and default is tcp. If there is only one
      port mapping this will be used by default and it will be enough to specify only the service
      name, e.g. -r wordpress. (default: [])
```

The port name referred to above is the name you gave the port when you specified it in the `-p`
option.  Your discovery service may or may not use the `protocol` part of the specification, in
which case leave it out and it will default to `http` but then be ignored by the plugin.

As a Helios user (as opposed to the people spinning up Helios clusters), ideally this is all you
need to know.

# Configuring the Agent
For agents to be able to register containers, the agent needs to know how to talk to your
service registration system.  This is done by command-line arguments to the agent.

* `--service-registrar-plugin SERVICE_REGISTRAR_PLUGIN` The tells the agent where the service
    registrar plugin jar lives.
* `--name NAME` Tells the agent which hostname to register as -- that is, the hostname it will put
    into the discovery service.  This is also the name that the agent
    is known by in the rest of the helios cluster.
* `--domain DOMAIN` The domain that the registrar should register services in by default.
* `--service-registry SERVICE_REGISTRY`  This is the address of where the service registrar plugin
    should talk to.  As far as Helios is concerned, this is an opaque string that is passed to the
    plugin, and not interpreted by Helios itself at all.

The documentation for the plugin you use should have documentation as to what to use for these.

# The Plugin API

The Plugin API that you need to implement is covered by two interfaces:
`com.spotify.helios.serviceregistration.ServiceRegistrarFactory`
and `com.spotify.helios.serviceregistration.ServiceRegistrar`.

The `ServiceRegistrarFactory` interface is pretty straightforward.  It has a pair of create methods,
`create` and `createForDomain`.  If you pass the `--service-registry` to the agent, `create` gets
called with its value, otherwise `createForDomain` gets called with the value of the argument to
`--domain`.  The `createForDomain` method is really only useful
if the address of your registrar is derivable solely by the domain.  If you pass
`--service-registry`, you will still get the domain in which you should register things,
just differently.  The return value of both of these methods is a `ServiceRegistrar`

There are two main methods on the registrar itself.

The first is `register` which takes a
`com.spotify.helios.serviceregistration.ServiceRegistration`.  `ServiceRegistration` objects are
simple wrappers around a `List<Endpoint>`, which can be obtained via `registration.getEndpoints()`.
An `Endpoint` should contain everything you need to register with your discovery service (though let
us know if this is not true for you).  It contains things like the name of the port, the protocol,
the port number itself, the domain in which to register and the hostname that we will advertise for
the port.

Based upon the information contained in the `Endpoint`s, the plugin does it's thing (whatever that
may be), and returns a `com.spotify.helios.serviceregistration.ServiceRegistrationHandle`.  The
handle interface is entirely empty, that is, as far as Helios is concerned, it's entirely opaque, so
you can do whatever you need to do whatever bookkeeping is required.

The second main method in the registrar is `unregister`.  This method takes one of the
`ServiceRegistrationHandle`s that you returned in `register` and in it you tell your service
discovery to remove the endpoints from what it knows.

Lastly, there is a close method, which the Agent will call when it is shutting down.  Obviously, if
it is killed, this close method may not be called.

The "pipes" to get configuration information to the plugin are a bit narrow.  So if you need
other configuration parameters to get to the plugin, you have a few options:

1. you can turn the `--service-registry` argument into a database-server like connect string
2. environment variables
3. configuration file in a well known location, possibly pointed to by one of the above options

The SkyDNS plugin goes with option #2.

# Master Registration And Configuration

Since the CLI needs to talk to the Helios masters, they too can usefully register with service
registration if it exports names somehow via DNS SRV records.  The arguments used to configure
registration are the same as for the agent. The Helios master itself will register with service
`helios` and protocol `http`.

The master does not register or handle HTTPS. If you want to speak HTTPS to the master, put nginx
in front of it and create a `_helios._https.<domain>` SRV record.

The CLI has an environment argument named `--srv-name` to specify the SRV name it should
look up in DNS as well as the `-d` option to specify the domain.  It takes both of those
and puts fills them into the `HELIOS_HTTPS_SRV_FORMAT` and `HELIOS_HTTPS_SRV_FORMAT` environment
variables format strings, which default to
`_%s._https.%s` and `_%s._http.%s`, respectively. The client then looks up the SRV records for the
resulting HTTPS SRV record (`_%s._https.%s`) with a fallback to HTTP (`_%s._http.%s`).

If you don't supply `--srv-name` it defaults to the string `helios`.
So by default if you ran `helios -d example.com command`,
the client will look up the SRV records `_helios._https.example.com`. If none exist, the
client will fallback to looking up the SRV records for `_helios._https.example.com`.
