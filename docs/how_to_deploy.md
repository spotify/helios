##Components

* Zookeeper >= 3.4.5 (the package in Ubuntu LTS 14.04 matches)

* Master - (`helios-master` process) The API for the CLI and HTTP endpoints. Communicates with the agent through Zookeeper. Provided by the `helios-master` deb.

* Agent (`helios-agent` process) Runs on each Docker host and communicates with Zookeeper, in charge of starting/stopping containers and reporting state back to Zookeeper. Provided by the `helios-agent` deb.

* Client (`helios` cli) A CLI client for interacting with the Helios master. Provided by the `helios` deb.

Helios processes are provided by a package matching the process name.

## Installing

Download the debs from the latest release https://github.com/spotify/helios/releases. The CLI can be installed from a single package. The master and agent packages require the `helios-services` package as a dependency. Services should not start after the packages are installed as they require configuration.

## Configuration

With the provided init scripts each Helios process can be configured with an
environment variable including the command line options that follow.  The init
files will try to source a file within `/etc/default` corresponding to the
process name. Each require `ENABLED=true` (actually any non-null string) defined
within the defaults files to actually start the process.

If you are not using the provided init files then you can pass these arguments
to the CLI directly. To set JVM options you must still use the corresponding
JVM options for each process.

### Master

Uses `HELIOS_MASTER_OPTS` environment variable for command line options. You
can specify `HELIOS_MASTER_JVM_OPTS` for additional JVM options


Takes options:

  `--name NAME`            hostname to register as (default: system's fqdn)

  `--domain DOMAIN`        Service registration domain.

  `--service-registry`     Service registry address. Overrides domain.

  `--service-registrar-plugin PATH_TO_PLUGIN` Service registration plugin used to register the master with an external service (EtcD for example). These are not well documented yet or open sourced.

  `--zk ZK`                zookeeper connection string (default: localhost:2181) Can include multiple zookeeper hosts (example: `zookeeper1.example.com:2181,zookeeper2.example.com:2181,zookeeper3.example.com:2181`).

  `--zk-session-timeout ZK_SESSION_TIMEOUT` zookeeper session timeout (default: 60000)

  `--zk-connection-timeout ZK_CONNECTION_TIMEOUT` zookeeper connection timeout (default: 15000)

  `--no-metrics` Turn off all collection and reporting of metrics (default: false)

  `--statsd-host-port STATSD_HOST_PORT` host:port of where to send statsd metrics (to be useful, --no-metrics must *NOT* be specified)

  --riemann-host-port RIEMANN_HOST_PORT host:port of where to send riemann events and metrics (to be useful, --no-metrics must *NOT* be specified)

  `-v, --verbose` (default: 0)

  `--syslog` Log to syslog. (default: false)

  `--logconfig LOGCONFIG` Logback configuration file.

  `--sentry-dsn SENTRY_DSN` The sentry data source name (For http://getsentry.com)

  `--http HTTP` http endpoint (default: http://0.0.0.0:5801)

  `--admin ADMIN` admin http port (default: 5802)

Example `/etc/default/helios-master`

    ENABLED=true

    HELIOS_MASTER_OPTS="--syslog \
        --zk zookeeper1.example.com:2181,zookeeper2.example.com:2181,zookeeper3.example.com:2181
        --riemann-host-port udp:localhost:5555 \
        --sentry-dsn SENRTY_DSN \
        --statsd-host-port localhost:8125 \
    "

    HELIOS_MASTER_JVM_OPTS="-Xms256m"


### Agent

Uses `HELIOS_AGENT_OPTS` environment variable for command line options. You can
specify `HELIOS_AGENT_JVM_OPTS` for additional JVM options


Takes options:

  `--name NAME`            hostname to register as (default system's fqdn)

  `--domain DOMAIN`        Service registration domain.

  `--service-registry SERVICE_REGISTRY`
                         Service registry address. Overrides domain.

  `--service-registrar-plugin PATH_TO_PLUGIN` Service registration plugin used to register running conatiners with an external service (EtcD for example). These are not well documented yet or open sourced.

  `--zk ZK`                zookeeper connection string (default: localhost:2181) Can include multiple zookeeper hosts (example: `zookeeper1.example.com:2181,zookeeper2.example.com:2181,zookeeper3.example.com:2181`).

  `--zk-session-timeout ZK_SESSION_TIMEOUT`
                         zookeeper session timeout (default: 60000)

  `--zk-connection-timeout ZK_CONNECTION_TIMEOUT`
                         zookeeper connection timeout (default: 15000)

  `--no-metrics`           Turn off all collection and  reporting of metrics (default:
                         false)

  `--statsd-host-port STATSD_HOST_PORT`
                         host:port of where to send statsd metrics (to be useful, --
                         no-metrics must *NOT* be specified)

  `--riemann-host-port RIEMANN_HOST_PORT`
                         host:port of where to send  riemann  events and metrics (to
                         be useful, --no-metrics must *NOT* be specified)

  `-v, --verbose`          (default: 0)

  `--syslog`               Log to syslog. (default: false)

  `--logconfig LOGCONFIG`  Logback configuration file.

  `--sentry-dsn SENTRY_DSN` The sentry data source name (For http://getsentry.com)

  `--no-http`              disable http server (default: false)

  `--http HTTP`            http endpoint (default: http://0.0.0.0:5803)

  `--admin ADMIN`          admin http port (default: 5804)

  `--id ID`                Agent unique ID. Generated  and  persisted  on first run if
                         not specified.

  `--state-dir STATE_DIR`  Directory for persisting agent state locally. (default: .)
  `--docker DOCKER`        docker endpoint (default: http://localhost:4160)

  `--env ENV [ENV ...]`    Specify environment variables that  will  pass  down to all
                         containers (default: [])

  `--syslog-redirect-to SYSLOG_REDIRECT_TO`
                         redirect container's  stdout/stderr  to  syslog  running at
                         host:port

  `--port-range PORT_RANGE`
                         Port   allocation   range,   start:end   (end   exclusive).
                         (default: 20000:32768)

Example `/etc/default/helios-agent

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

## Monitoring Helios

The master and agent processes have a `/healthcheck` endpoint that can be hit in order to monitor the process. These run within the HTTP endpoint for each process and as such require hitting the same port specified in the CLI.

Master with default port

    $ curl http://localhost:5802/healthcheck
    * deadlocks: OK
    * zookeeper: OK

Agent with default port

    $ curl http://localhost:5804/healthcheck
    * deadlocks: OK
    * docker: OK
    * zookeeper: OK
