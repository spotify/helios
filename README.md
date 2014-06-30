Helios
======

[![Build Status](https://travis-ci.org/spotify/helios.svg?branch=master)](https://travis-ci.org/spotify/helios)

Helios is a Docker orchestration platform for deploying and managing
containers across an entire fleet.

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

Danger, Will Robinson!
----------------------
This is **not** production-ready software! Please do not deploy it in
mission-critical scenarios, or you will make [the panda](https://www.facebook.com/spotify.panda)
very sad.

Getting Started
---------------

If you're looking for how to use Helios, see the [wiki](https://github.com/spotify/helios/wiki).
Most probably the [User Manual](https://github.com/spotify/helios/wiki/Helios-User-Manual)
is what you're looking for.

If you're looking for how to build, install and run Helios, keep reading.

Prerequisities
--------------

* [Docker 1.0](https://github.com/dotcloud/docker) or newer
* [Zookeeper 3.4.0](https://zookeeper.apache.org/) or newer


Build & Test
------------

First, make sure you have Docker installed locally. If you're using OS X, you can use
the included Vagrantfile to bring up Docker inside of a VM:

```sh
$ vagrant up

# set DOCKER_HOST to use the Docker instance inside the VM
$ export DOCKER_HOST=tcp://192.168.33.10:2375
```

Actually building Helios and running its tests should be a simple matter
of running:

    $ mvn clean test

Install & Run
-------------

### Quick start
If you have [Vagrant](http://www.vagrantup.com/) installed locally, just run:

    $ vagrant up

This will start a virtual machine, install the prereqs, build the project, and install and run
the Helios agent and master. You can then use the Vagrant environment:
```
$ vagrant ssh
vagrant@ubuntu-14:~$ helios hosts
HOST             STATUS    DEPLOYED    RUNNING    CPUS    MEM     LOAD AVG    MEM USAGE    OS       VERSION
192.168.33.10    Down      0           0          2       0 gb    0.09        0.81         Linux    3.13.0-24-generic
```

Note that the included Vagrantfile doesn't run the test suite when building Helios. This is so
we can get you up and running with a VM as quickly as possible. You should run `mvn package`
yourself to run the test suite.

### Manual approach

The launcher scripts are in [bin/](https://github.com/spotify/helios/tree/master/bin).
After you've run `mvn package`, you should be able to start the agent and master:

    $ bin/helios-master &
    $ bin/helios-agent &

If you see any issues, make sure you have the prerequisites (Docker and Zookeeper) installed.

### Production

Note that Helios has **not** been tested in production. That said, you can
follow these instructions for test deployments.

Running `mvn package` generates Debian packages that you can install on
your servers. For example:

    $ mvn package
    ...
    $ find . -name "*.deb"
    ./helios-services/target/helios-agent_0.0.27-SNAPSHOT_all.deb
    ./helios-services/target/helios-master_0.0.27-SNAPSHOT_all.deb
    ./helios-services/target/helios-services_0.0.27-SNAPSHOT_all.deb
    ./helios-tools/target/helios_0.0.27-SNAPSHOT_all.deb

The Debian package in the helios-tools directory contains the Helios CLI tool.

The `helios-services` Debian package is a shared dependency of both the agent
and the master. Install it and either the `helios-agent` or `helios-master`
Debian package on each agent and master, respectively.

Findbugs
--------

To run [findbugs](http://findbugs.sourceforge.net) on the helios codebase, do
`mvn clean compile site`. This will build helios and then run an analysis,
emitting reports in `helios-*/target/site/findbugs.html`.

To silence an irrelevant warning, add a filter match along with a justification
in `findbugs-exclude.xml`.

The Nickel Tour
---------------

The sources for the Helios master and agent are under [helios-services](https://github.com/spotify/helios/tree/master/helios-services).
The CLI source is under [helios-tools](https://github.com/spotify/helios/tree/master/helios-tools).
The Helios Java client is under [helios-client](https://github.com/spotify/helios/tree/master/helios-client).

The main meat of the Helios agent is in [Supervisor.java](https://github.com/spotify/helios/blob/master/helios-services/src/main/java/com/spotify/helios/agent/Supervisor.java),
which revolves around the lifecycle of managing individual running Docker containers.

For the master, the HTTP response handlers are in [src/main/java/com/spotify/helios/master/resources](https://github.com/spotify/helios/tree/master/helios-services/src/main/java/com/spotify/helios/master/resources).

Interactions with ZooKeeper for the agent and master are mainly in [ZookeeperAgentModel.java](https://github.com/spotify/helios/blob/master/helios-services/src/main/java/com/spotify/helios/agent/ZooKeeperAgentModel.java)
and [ZooKeeperMasterModel.java](https://github.com/spotify/helios/blob/master/helios-services/src/main/java/com/spotify/helios/master/ZooKeeperMasterModel.java),
respectively.

The Helios services use [Dropwizard](http://dropwizard.io) which is a
bundle of Jetty, Jersey, Jackson, Yammer Metrics, Guava, Logback and
other Java libraries.

Releasing
---------

    # Run tests and create a tagged release commit
    ./release.sh

    # Push it
    ./push-release.sh
