Using helios-solo
===

helios-solo provides a local Helios cluster running in a Docker container. This
gives you a single Helios master and agent to play around with. All Helios jobs
are run on the local Docker instance. The only prerequisite is
[Docker](https://docs.docker.com/installation/).

Install & Run
---

### OS X

Install [docker-machine](https://docs.docker.com/machine/) or
another similar solution. Configure it correctly and **start the service** so that commands like
`docker info` and `docker ps` work. Then run the below.

```bash
$ brew tap spotify/public && brew install helios-solo
$ helios-up
```

### Linux with Systemd

Install Docker, and enable socket-activation for it:

```bash
$ sudo systemctl enable docker.socket
Created symlink from /etc/systemd/system/sockets.target.wants/docker.socket to /usr/lib/systemd/system/docker.socket.
$ sudo systemctl start docker.socket
```

Verify that socket activation works:

```bash
$ systemctl status docker
● docker.service - Docker Application Container Engine
   Loaded: loaded (/usr/lib/systemd/system/docker.service; disabled; vendor preset: disabled)
   Active: inactive (dead)
     Docs: https://docs.docker.com
$ docker version -f '{{ .Server.Version }}'
1.8.3
$ systemctl status docker
● docker.service - Docker Application Container Engine
   Loaded: loaded (/usr/lib/systemd/system/docker.service; disabled; vendor preset: disabled)
   Active: active (running) since tor 2015-10-29 20:01:09 CET; 1min 34s ago
     Docs: https://docs.docker.com
 Main PID: 27126 (docker)
   CGroup: /system.slice/docker.service
           └─27126 /usr/bin/docker daemon -H fd:// --exec-opt native.cgroupdriver=cgroupfs
```

Now drop in the service definition file for `helios-solo`:

```bash
$ sudo curl https://raw.githubusercontent.com/spotify/helios/master/solo/helios-solo.service \
  -o /etc/systemd/system/helios-solo.service
$ sudo systemctl daemon-reload
$ sudo systemctl enable helios-solo
Created symlink from /etc/systemd/system/multi-user.target.wants/helios-solo.service to /etc/systemd/system/helios-solo.service.
$ sudo systemctl start helios-solo
● helios-solo.service - Spotify Helios Solo
   Loaded: loaded (/usr/lib/systemd/system/helios-solo.service; enabled; vendor preset: disabled)
   Active: active (running) since tor 2015-10-29 20:08:49 CET; 4s ago
     Docs: https://github.com/spotify/helios/tree/master/docs
  Process: 27578 ExecStartPre=/usr/bin/docker pull spotify/helios-solo (code=exited, status=0/SUCCESS)
  Process: 27570 ExecStartPre=/usr/bin/docker rm helios-solo-container (code=exited, status=0/SUCCESS)
  Process: 27562 ExecStartPre=/usr/bin/docker kill helios-solo-container (code=exited, status=1/FAILURE)
 Main PID: 27586 (docker)
   CGroup: /system.slice/helios-solo.service
           └─27586 /usr/bin/docker run --name=helios-solo-container ...
```

### Ubuntu

Install Docker, configure it correctly, and **start the service** so that
commands like `docker info` and `docker ps` work. Either
[follow the detailed instructions from Docker](https://docs.docker.com/installation/),
or opt for the quick install:

```bash
$ curl -sSL https://get.docker.com/ | sudo sh -
$ sudo usermod -aG docker `whoami`
```

Then, install helios-solo:

```bash
$ curl -sSL https://spotify.github.io/helios-apt/go | sudo sh -
$ sudo apt-get install helios-solo
$ helios-up
```

If `helios-up` fails, ensure that Docker is running and your client is correctly
configured. A good test is to run `docker info` and make sure it works.

Usage
---

Here are some example commands:

```bash
# Start helios-solo
$ helios-up

# Show available versions of Helios to use for helios-solo
$ helios-use

# Upgrade to the latest version of Helios
$ helios-use latest

# Clean up all jobs & containers
$ helios-cleanup
```

Once helios-solo is up, you can use the `helios-solo` command to talk to it. The
usage is identical to the `helios` CLI. Here's an example:

```bash
# Make sure the helios-solo agent is up
$ helios-solo hosts

# Create a trivial helios job
$ helios-solo create test:1 spotify/busybox:latest -- sh -c "while :; do sleep 1; done"

# Deploy the job on the (local) solo host
$ helios-solo deploy test:1 solo

# Check the job status
$ helios-solo status

# Undeploy job
$ helios-solo undeploy -a --yes test:1

# Remove job
$ helios-solo remove test:1
```

Commands
--------

* `helios-up`<br />
  Brings up the helios-solo container.

* `helios-down`<br />
  Destroys the helios-solo container.

* `helios-solo ...`<br />
  Wrapper around the helios CLI for talking to helios-solo.
  Essentially identical to `eval $(helios-env) && helios -z $HELIOS_URI ...`.

* `helios-restart`<br />
  Destroy and restart the helios-solo container.

* `helios-cleanup`<br />
  Remove all helios-solo jobs and containers.

* `helios-env`<br />
  Utility for setting `HELIOS_URI` environment variable in your
  shell: `eval $(helios-env)`.

* `helios-use` [version]<br />
  Lists the available versions of Helios, switches the underlying Helios version, or upgrades to the latest version.

Container Logging
-----------------

To see the logs for a container deployed by helios-solo, run:

    $ docker logs <container_id>

You can get the ID's of all running containers with `docker ps`.

Debugging
---------

The following commands can be helpful for debugging helios-solo:

```bash
$ docker logs helios-solo-container

$ helios-up && docker exec -it helios-solo-container bash
```
