## Components

* **Docker >= 1.0**<br />
  The `lxc-docker` package in Ubuntu LTS 14.04 works.

* **Zookeeper >= 3.4.5**<br />
  The `zookeeperd` package in Ubuntu LTS 14.04 works.

* **Helios master**<br />
  Provided by the `helios-master` package. The API for the CLI and HTTP endpoints. Communicates with the agent through Zookeeper.

* **Helios agent**<br />
  Provided by the `helios-agent` package. Runs on each Docker host and communicates with Zookeeper. In charge of starting/stopping containers and reporting state back to Zookeeper.

* **Helios command-line tools**<br />
  Provided by the `helios` package. A CLI client for interacting with the Helios master.

## Installing

Add the Helios apt repository to get Debian packages:

    $ curl -sSL https://spotify.github.io/helios-apt/go | sudo sh -

You can then `apt-get install helios`, `helios-agent`, and `helios-master`.
Note that the Helios master and agent services both try to connect to ZooKeeper at `localhost:2181`
when installed. You will need to configure and enable them as detailed below.

### Whatever, just get me running

This will install and start the Helios master and Helios agent on a single machine with default
configuration:

```bash
# install prereqs
$ sudo apt-get install zookeeperd docker.io

# install helios
$ curl -sSL https://spotify.github.io/helios-apt/go | sudo sh -
$ sudo apt-get install helios helios-agent helios-master

# check if it worked and the local agent is registered
$ helios -z http://localhost:5801 hosts
```

## Configuration

The `helios-agent` and `helios-master` packages will install defaults files:

* Agent: `/etc/default/helios-agent`
* Master: `/etc/default/helios-master`

You can set `HELIOS_AGENT_OPTS` and `HELIOS_MASTER_OPTS` in these files. See the options below.

### Helios master options
Specify these options in the `HELIOS_MASTER_OPTS` variable in `/etc/default/helios-master`:

* `--name NAME`
  hostname to register as (default: system FQDN)

* `--zk ZK`
  zookeeper connection string (default: localhost:2181) Can include multiple zookeeper hosts (example: `zookeeper1.example.com:2181,zookeeper2.example.com:2181,zookeeper3.example.com:2181`).

* `--zk-session-timeout ZK_SESSION_TIMEOUT`
  Optional. Zookeeper session timeout (default: 60000)

* `--zk-connection-timeout ZK_CONNECTION_TIMEOUT`
  Optional. Zookeeper connection timeout (default: 15000)

* `--no-metrics`
  Turn off all collection and reporting of metrics (default: false)

* `--domain DOMAIN`
  Optional. Service registration domain. Used for the master to register itself with the service registrar.

* `--service-registry`
  Optional. Service registry address. Overrides domain. Used for the master to register itself with
  the service registrar.

* `--service-registrar-plugin PATH_TO_PLUGIN`
  Optional. Service registration plugin used to register the master with an external service (etcd
  for example). These are not well documented yet. Used for the master to register itself with the
  service registrar.

* `--statsd-host-port STATSD_HOST_PORT`
  Optional. The host:port of where to send statsd metrics (to be useful, --no-metrics must *NOT* be specified)

* --riemann-host-port RIEMANN_HOST_PORT
  Optional. The host:port of where to send riemann events and metrics (to be useful, --no-metrics must *NOT* be specified)

* `-v, --verbose`
  (default: 0)

* `--syslog`
  Log to syslog. (default: false)

* `--logconfig LOGCONFIG`
  Logback configuration file.

* `--sentry-dsn SENTRY_DSN`
  Optional. The sentry data source name (For http://getsentry.com)

* `--http HTTP`
  http endpoint (default: http://0.0.0.0:5801) the master will listen on.

* `--admin ADMIN`
  admin http port (default: 5802) the master will listen on.

### Helios agent options
Specify these options in the `HELIOS_AGENT_OPTS` variable in `/etc/default/helios-agent`:

* `--name NAME`
  hostname to register as (default: system FQDN)

* `--zk ZK`
  zookeeper connection string (default: localhost:2181). Can include multiple zookeeper hosts
  (example: `zookeeper1.example.com:2181,zookeeper2.example.com:2181,zookeeper3.example.com:2181`).

* `--zk-session-timeout ZK_SESSION_TIMEOUT`
  Optional. Zookeeper session timeout (default: 60000)

* `--zk-connection-timeout ZK_CONNECTION_TIMEOUT`
  Optional. Zookeeper connection timeout (default: 15000)

* `--no-metrics`
  Turn off all collection and reporting of metrics (default: false)

* `--domain DOMAIN`
  Optional. Service registration domain. Used for the agent to register deployed jobs in the service registrar.

* `--service-registry SERVICE_REGISTRY`
  Optional. Service registry address. Used for the agent to register deployed jobs in the service registrar.

* `--service-registrar-plugin PATH_TO_PLUGIN`
  Optional. Service registration plugin used to register running conatiners with an external service
  (etcd for example). These are not well documented yet. Used for the agent to register deployed jobs in the service registrar.

* `--no-metrics`
  Turn off all collection and  reporting of metrics (default: false)

* `--statsd-host-port STATSD_HOST_PORT`
  Optional. The host:port of where to send statsd metrics (to be useful, --no-metrics must *NOT* be specified)

* `--riemann-host-port RIEMANN_HOST_PORT`
  Optional. The host:port of where to send  riemann  events and metrics (to be useful, --no-metrics must *NOT* be specified)

* `-v, --verbose`
  (default: 0)

* `--syslog`
  Log to syslog. (default: false)

* `--logconfig LOGCONFIG`
  Logback configuration file.

* `--sentry-dsn SENTRY_DSN`
  Optional. The sentry data source name (For http://getsentry.com)

* `--no-http`
  Disable http server (default: false)

* `--http HTTP`
  The http endpoint (default: http://0.0.0.0:5803) to listen on.

* `--admin ADMIN`
  The admin http port (default: 5804) to listen on.

* `--id ID`
  Agent unique ID. Generated  and  persisted  on first run if not specified.

* `--state-dir STATE_DIR`
  Directory for persisting agent state locally. (default: .)

* `--docker DOCKER`
  Docker endpoint (default: detected from `DOCKER_HOST` environment variable)

* `--docker-cert-path DOCKER_CERT_PATH`
  Directory containing client.pem and client.key or connecting to Docker over HTTPS.

* `--env ENV [ENV ...]`
  Specify environment variables that will pass down to all containers (default: [])

* `--syslog-redirect-to SYSLOG_REDIRECT_TO`
  Optional. Redirect container stdout/stderr to syslog running at host:port.

* `--port-range PORT_RANGE`
  Port allocation range, start:end (end exclusive). (default: 20000:32768)

* `--bind VOLUME`
  Bind the given volume into all containers. You may specify multiply `--bind` arguments. Each bind
  must conform to the [`docker run -v` syntax](https://docs.docker.com/reference/run/#volume-shared-filesystems).

### Examples

Example `/etc/default/helios-master`:

```
ENABLED=true

HELIOS_MASTER_OPTS="--syslog \
    --zk zookeeper1.example.com:2181,zookeeper2.example.com:2181,zookeeper3.example.com:2181
    --riemann-host-port udp:localhost:5555 \
    --sentry-dsn SENRTY_DSN \
    --statsd-host-port localhost:8125 \
"

HELIOS_MASTER_JVM_OPTS="-Xms256m"
```

Example `/etc/default/helios-agent`:

```
ENABLED=true

HELIOS_AGENT_OPTS="--syslog \
    --zk zookeeper1.example.com:2181,zookeeper2.example.com:2181,zookeeper3.example.com:2181
    --riemann-host-port udp:localhost:5555 \
    --sentry-dsn SENRTY_DSN \
    --statsd-host-port localhost:8125 \
    --state-dir /var/lib/helios-agent \
    --env CONTAINER_ENV_01=VALUE_01 \
CONTAINER_ENV_02=VALUE_02 \
"

HELIOS_AGENT_JVM_OPTS="-Xmx256m"
```

## Monitoring Helios

The master and agent processes have a `/healthcheck` endpoint that can be hit in order to monitor
the process.

Master with default port:

    $ curl http://localhost:5802/healthcheck
    {"deadlocks":{"healthy":true},"zookeeper":{"healthy":true}}

Agent with default port:

    $ curl http://localhost:5804/healthcheck
    {"deadlocks":{"healthy":true},"docker":{"healthy":true},"zookeeper":{"healthy":true}}
