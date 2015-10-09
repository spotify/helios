Helios [![Circle CI](https://circleci.com/gh/spotify/helios/tree/master.png?style=badge)](https://circleci.com/gh/spotify/helios/tree/master) [![Slack Status](http://slackin.spotify.com/badge.svg)](http://slackin.spotify.com) [![Searchable with Codota](https://img.shields.io/badge/Codota-available-brightgreen.svg)](http://www.codota.com/xref/#/github_spotify_helios_5612e5f19b224403007e2fd6/findUsages)
======

Helios is a Docker orchestration platform for deploying and managing
containers across an entire fleet of servers. Helios provides a HTTP
API as well as a command-line client to interact with servers running
your containers. It also keeps history of events in your cluster including
information such as deploys, restarts and version changes.


Usage Example
-------------

```sh
# Create an nginx job using the nginx container image, exposing it on the host on port 8080
$ helios create nginx:v1 nginx:1.7.1 -p http=80:8080

# Check that the job is listed
$ helios jobs

# List helios hosts
$ helios hosts

# Deploy the nginx job on one of the hosts
$ helios deploy nginx:v1 <host>

# Check the job status
$ helios status

# Curl the nginx container when it's started running
$ curl <host>:8080

# Undeploy the nginx job
$ helios undeploy -a nginx:v1

# Remove the nginx job
$ helios remove nginx:v1
```

Getting Started
---------------

If you're looking for how to use Helios, see the [docs directory](docs).
Most probably the [User Manual](docs/user_manual.md) is what you're looking for.

If you're looking for how to download, build, install and run Helios, keep reading.

Prerequisities
--------------

The binary release of Helios is built for Ubuntu 14.04.1 LTS, but Helios should
be buildable on any platform with at least Java 7 and a recent Maven 3
available.

Other components are that are required for a helios installation are:

* [Docker 1.0](https://github.com/dotcloud/docker) or newer
* [Zookeeper 3.4.0](https://zookeeper.apache.org/) or newer


Install & Run
-------------

### Quick start for local usage
Use [helios-solo](https://github.com/spotify/helios/blob/master/docs/helios_solo.md)
to launch a local environment with a Helios master and agent.

First, ensure you have [Docker installed locally](https://docs.docker.com/installation/).
Test this by making sure `docker info` works. Then install helios-solo:

```bash
# install helios-solo on Debian/Ubuntu
$ curl -sSL https://spotify.github.io/helios-apt/go | sudo sh -
$ sudo apt-get install helios-solo

# install helios-solo on OS X
$ brew tap spotify/public && brew install helios-solo
```

Once you've got it installed, bring up the helios-solo cluster:

```bash
# launch a helios cluster in a Docker container
$ helios-up

# check if it worked and the solo agent is registered
$ helios-solo hosts
```

You can now [use helios-solo](https://github.com/spotify/helios/blob/master/docs/helios_solo.md#usage)
as your local Helios cluster. If you have issues, see [the detailed helios-solo documentation](https://github.com/spotify/helios/blob/master/docs/helios_solo.md).

### Production on Debian, Ubuntu, etc.

Prebuilt Debian packages are available for production use. To install:

```bash
$ curl -sSL https://spotify.github.io/helios-apt/go | sudo sh -

# install Helios command-line tools
$ sudo apt-get install helios

# install Helios master (assumes you have zookeeperd installed)
$ sudo apt-get install helios-master

# install Helios agent (assumes you have Docker installed)
$ sudo apt-get install helios-agent
```

Note that the Helios master and agent services both try to connect to ZooKeeper at `localhost:2181`
by default. We recommend reading [the Helios configuration & deployment guide](https://github.com/spotify/helios/blob/master/docs/how_to_deploy.md)
before starting a production cluster.

#### Whatever, just get me running

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

### Manual approach

The launcher scripts are in [bin/](bin). After you've built Helios following the
instructions below, you should be able to start the agent and master:

    $ bin/helios-master &
    $ bin/helios-agent &

If you see any issues, make sure you have the prerequisites (Docker and Zookeeper) installed.

Build & Test
------------

First, make sure you have Docker installed locally. If you're using OS X, we
recommend using [docker-machine](https://docs.docker.com/machine/).

Actually building Helios and running its tests should be a simple matter
of running:

    $ mvn clean package

How it all fits together
------------------------

The `helios` command line tool connects to your helios master via HTTP. The
Helios master is connected to a Zookeeper cluster that is used both as
persistent storage and as a communications channel to the agents. The
helios agent is a java process that typically lives on the same host as
the Docker daemon, connecting to it via a Unix socket or optionally TCP
socket.

Helios is designed for high availability, with execution state being confined
to a potentially highly available Zookeeper cluster. This means that several
helios-master services can respond to HTTP requests concurrently, removing
any single point of failure in the helios setup using straight forward HTTP
load balancing strategies.


Production Readiness
--------------------
We at Spotify are running Helios in production (as of October 2015) with dozens
of critical backend services, so we trust it.  Whether you should trust it to
not cause smoking holes in your infrastructure is up to you.


Why Helios?
-----------

There are a number of Docker orchestration systems, why should you
choose Helios?

* Helios is pragmatic.  We're not trying to solve everything *today*,
  but what we have, we try hard to ensure is rock-solid.  So we don't
  have things like resource limits or dynamic scheduling yet.  Today,
  for us, it has been more important to get the CI/CD use cases, and
  surrounding tooling solid first.  That said, we eventually want to
  do dynamic scheduling, composite jobs, etc. (see below for more).
  But what we provide, we use (i.e. we eat our own dogfood), so you
  can have reasonable assurances that anything that's been in the
  codebase for more than a week or two is pretty solid as we release
  frequently (usually, at least weekly) into production here at
  Spotify.

* Helios should be able to fit in the way you already do ops.  Of the
  popular Docker orchestration frameworks, Helios is the only one
  we're aware of that doesn't have anything much in the way of system
  dependencies.  That is, we don't require that you run in AWS or GCE,
  etc.  We don't require a specific network topology.  We don't
  require you run a specific operating system.  We don't require that
  you're using Mesos.  Our only requirement is that you have a
  ZooKeeper cluster somewhere and a Java 7 JVM on the machines which
  Helios runs on.  So if you're using Puppet, Chef, etc., to manage the
  rest of the OS install and configuration, you can still continue to
  do so with whatever Linux OS you're using.

* Don't have to drink *all* the Kool-Aid.  Generally, we try to make
  it so you only have to take the features you want to use, and should
  be able to ignore the rest.  For example, Helios doesn't prescribe a
  discovery service: we happen to provide a plugin for SkyDNS, and we
  hear that someone else is working on one for another service, but if
  you don't want to even use a discovery service, you don't have to.

* Scalability.  We're already at hundreds of machines in
  production, but we're nowhere near the limit before the existing
  architecture would need to be revisited.  Helios can also scale down
  well in that you can run a single machine instance if you want to
  run it all locally.

Other Software You Might Want To Consider
-----------------------------------------
Here are a few other things you probably want to consider using alongside
Helios:
* [docker-gc](https://github.com/spotify/docker-gc) Garbage collects dead containers and removes unused images.
* [syslog-redirector](https://github.com/spotify/syslog-redirector) Can be used by Helios agents to redirect the standard out/err of containers to syslog.
* [helios-skydns](https://github.com/spotify/helios-skydns) Makes it so you can auto register services in SkyDNS.  If you use leading underscores in your SRV record names, let us know, we have a patch for etcd which disables the "hidden" node feature which makes this use case break.
* [skygc](https://github.com/spotify/skygc)  When using SkyDNS, especially if you're using the Helios Testing Framework, can leave garbage in the skydns tree within etcd.  This will clean out dead stuff.
* [docker-maven-plugin](https://github.com/spotify/docker-maven-plugin)  Simplifies the building of Docker containers if you're using Maven (and most likely Java).

Findbugs
--------

To run [findbugs](http://findbugs.sourceforge.net) on the helios codebase, do
`mvn clean compile site`. This will build helios and then run an analysis,
emitting reports in `helios-*/target/site/findbugs.html`.

To silence an irrelevant warning, add a filter match along with a justification
in `findbugs-exclude.xml`.

The Nickel Tour
---------------

The sources for the Helios master and agent are under [helios-services](helios-services).
The CLI source is under [helios-tools](helios-tools).
The Helios Java client is under [helios-client](helios-client).

The main meat of the Helios agent is in [Supervisor.java](helios-services/src/main/java/com/spotify/helios/agent/Supervisor.java),
which revolves around the lifecycle of managing individual running Docker containers.

For the master, the HTTP response handlers are in [src/main/java/com/spotify/helios/master/resources](helios-services/src/main/java/com/spotify/helios/master/resources).

Interactions with ZooKeeper for the agent and master are mainly in [ZookeeperAgentModel.java](helios-services/src/main/java/com/spotify/helios/agent/ZooKeeperAgentModel.java)
and [ZooKeeperMasterModel.java](helios-services/src/main/java/com/spotify/helios/master/ZooKeeperMasterModel.java),
respectively.

The Helios services use [Dropwizard](http://dropwizard.io) which is a
bundle of Jetty, Jersey, Jackson, Yammer Metrics, Guava, Logback and
other Java libraries.

You may [search and navigate this codebase using Codota semantic code search](http://www.codota.com/xref/#/github_spotify_helios_5612e5f19b224403007e2fd6/findUsages).


Community Ideas
---------------

These are things we want, but haven't gotten to.  If you feel
inspired, we'd love to talk to you about these (in no particular
order):

* Host groups
* ACLs - on jobs, hosts, and deployments
* Composite jobs -- be able to deploy related containers as a unit on a machine
* Run once jobs -- for batch jobs
* Resource specification and enforcement -- That is: restrict my container to *X* MB of RAM, *X* CPUs, and *X* MB disk and perhaps other things like IOPs, network bandwidth, etc.
* Dynamic scheduling of jobs -- either within Helios itself or as a layer on top
* Packaging/Config for other Linux distributions such as RedHat, CoreOS, etc.
