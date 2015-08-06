# User Manual

This guide gives an overview of Helios, and what you need to know to deploy and run your Docker containers using it.

Note that this guide assumes that you are familiar with [Docker](http://docker.io) and concepts like images and containers. If you aren't familiar with Docker, see the [getting started page](https://www.docker.io/gettingstarted/).


* [Basic Concepts in Helios](#basic-concepts-in-helios)
* [Install the Helios CLI](#install-the-helios-cli)
* [Using the Helios CLI](#using-the-helios-cli)
* [Managing Helios Agents](#managing-helios-agents)
* [Creating Your Job](#creating-your-job)
  * [A basic job](#a-basic-job)
  * [Specifying a command to run](#specifying-a-command-to-run)
  * [Passing environment variables](#passing-environment-variables)
  * [Using a helios job config file](#using-a-helios-job-config-file)
  * [Health Checks](#health-checks)
  * [Specifying an Access Token](#specifying-an-access-token)
* [Deploying Your Job](#deploying-your-job)
  * [Checking deployment status and history](#checking-deployment-status-and-history)
  * [Undeploying](#undeploying)
  * [Using Deployment Groups](#using-deployment-groups)
* [Once Inside The Container](#once-inside-the-container)

Basic Concepts in Helios
---

* **Job:** A job configuration tells Helios how to run your Docker container. It consists of things like a job name, a job version, the name of your Docker image, any environment variables you wish to pass to the running container, ports you wish to expose, and the command to run inside the container, if any.

* **Master:** Helios masters are the servers that the Helios CLI and other tools talk to. They coordinate the deployment of your Docker containers onto Helios agents.

* **Agents:** Helios agents, sometimes known as Helios hosts, are the machines on which the images you built eventually run. The masters tell agents to download jobs and run them.

Install the Helios CLI
---

Whichever environment you are deploying to, you should install the CLI locally so you can talk to 
Helios clusters without having to SSH to another machine.

  * Ubuntu Trusty: `dpkg -i helios_all.deb` [download from here](https://github.com/spotify/helios/releases)
  * Mac OS X: `brew install helios` (after installing [Spotify's homebrew tap](https://github.com/spotify/homebrew-public))

Using the Helios CLI
---

The `helios` command is your primary interface for interacting with the Helios cluster.

When using the CLI, you'll have to specify which Helios endpoint you want to talk to. To talk to a specific Helios master, use the `-z` flag. For example, if you have a Helios master running locally on the default port:

    $ helios -z http://localhost:5801

If you have multiple masters, you can [setup automatic master lookup](automatic_master_lookup.md).

**Note:** that the rest of this manual uses the `helios` command without specifying either the `-z` or `-d` flag. This is only for simplicity, though you can emulate this behavior by aliasing the `helios` command. For example:

    alias helios=helios -z http://localhost:5801

This will save you from having to type the `-z` flag over and over again.

If you need general help, run `helios --help`. Many Helios CLI commands also take the `--json` option, which will produce JSON results instead of human-readable ones. This is useful if you want to script Helios or incorporate it into a build process.

## Managing Helios Agents

### Register and Unregister Agents

Register and unregister with `helios [un]register <hostname> <unqiue ID>`.

### List Agents

List agents with `helios hosts [optional hostname pattern]`. If your agents have labels like
`key=value` (see below on how to label agents), you can filter on labels with 
`helios hosts -l key1=value1 -b key2=value2`.

### Label Agents

You can add labels to your agents when starting the Helios agent process by passing in
`java AgentMain ... --labels key1=value1 --labels key2=value2`. Labels are used by
[deployment groups](#using-deployment-groups).

Creating Your Job
---
Once you have a Docker image you want to deploy, you need to tell Helios how you want it run. First we tell Helios the relevant details.

### A basic job

If you specified an `ENTRYPOINT` in the `Dockerfile` you built your image with, you can create your job with the `create` command. For example, to create a job named "foo" to run the `nginx` image:

    $ helios create foo:v1 nginx
    Creating job: {"id":"foo:v1:2a89d5a87851d68678aabdad38d31faa296f5bf1","image":"nginx","command":[],"env":{},"ports":{},"registration":{}}
    Done.
    foo:v1:2a89d5a87851d68678aabdad38d31faa296f5bf1

### Specifying a command to run

If you didn't specify an `ENTRYPOINT` in your Dockerfile, you can specify a command to run when the container is started. For example, to create a Ubuntu container that prints the time every 60 seconds:

    $ helios create testjob:1 ubuntu:12.04 -- \
        /bin/sh -c 'while true; do date; sleep 60; done'
    Creating job: {"command":["/bin/sh","-c","while true; do date; sleep 60;        
    done"],"env":{},"expires":null,"id":"testjob:1:4f7125bff35d3cecaac237da3ab17efca8a765f9",
    "image":"ubuntu:12.04","ports":{},"registration":{},"registrationDomain":"","volumes":{}}
    Done.
    testjob:1:da2fa2d26da6a8392536826631e58803e3fcc911

**NOTE**: When passing a command-line to your container, precede it with the double dash (`--`) as in the above example. While it might work sometimes without it, I wouldn't count on it, as the Helios CLI may misinterpret your intent.

### Passing environment variables

For some use cases, you may want to pass some environment variables to the job that aren't baked into the image. In which case you can do:

    $ helios create testjob:1 ubuntu:12.04 --env FOO=bar -- \
        /bin/sh -c 'while true; do echo $FOO; date; sleep 60; done'
    Creating job: {"id":"testjob:1:bad9111c6c9e10975408f2f41c561fd3849f55
    61","image":"ubuntu:12.04","command":["/bin/sh","-c","while true; do echo $FOO; date; sleep 60;
    done"],"env":{"FOO":"bar"}}
    Done.
    testjob:1:4f7125bff35d3cecaac237da3ab17efca8a765f9

The last line of output in the command output is the canonical job ID. Most times, you will only need the `jobName:jobVersion` parts, but in the event that you create two jobs with the same name and version, you can unambiguously choose which one you intend to operate on by supplying the full ID.

### Using a Helios job config file

`helios create -d <DOMAINS> -f <HELIOS_JOB_CONFIG_FILE_PATH> <MORE> <CLI> <ARGS>` will merge
job parameters in the file `<HELIOS_JOB_CONFIG_FILE_PATH>` with other command line arguments. CLI
args take precedence. The job configuration file should be valid JSON with a schema that matches the
output of `helios inspect -d <DOMAINS> <EXISTING_JOB_NAME> --json`. Here's an example that uses all
the available configuration keys with an explanation of each one.

```
{
  "command" : [ "server", "serverconfig.yaml" ],
  "env" : {
    "JVM_ARGS" : "-Ddw.feature.randomFeatureFlagEnabled=true"
  },
  "expires" : "2014-06-01T12:00:00Z",
  "gracePeriod": 60,
  "healthCheck" : {
    "type" : "http",
    "path" : "/healthcheck",
    "port" : "http-admin"
  },
  "id" : "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265",
  "image" : "myregistry:80/janedoe/myservice:0.5-98c6ff4",
  "hostname": "myhost",
  "networkMode" : "bridge",
  "ports" : {
    "http" : {
      "externalPort" : 8080,
      "internalPort" : 8080,
      "protocol" : "tcp"
    },
    "http-admin" : {
      "externalPort" : 8081,
      "internalPort" : 8081,
      "protocol" : "tcp"
    }
  },
  "registration" : {
    "fooservice/http" : {
      "ports" : {
        "http" : { }
      }
    }
  },
  "registrationDomain" : "",
  "securityOpt" : [ "label:user:USER", "apparmor:PROFILE" ],
  "token": "insecure-access-token",
  "volumes" : {
    "/destination/path/in/container.yaml:ro" : "/source/path/in/host.yaml"
  }
}
```

* `command`: The command line arguments to pass to the container (default: []).
* `env`: Environment variables (default: []).
* `expires`: An ISO-8601 string representing the date/time when this job should expire. The job
  will be undeployed from all hosts and removed at this time. E.g. 2014-06-01T12:00:00Z
  (default: null).
* `gracePeriod`: If is specified, Helios will unregister from service discovery and wait the specified
  number of seconds before undeploying, default 0 seconds.
* `healthCheck`: A health check Helios will execute on the container. See the health checks section
  below.
* `id`: The id of the job.
* `image`: The docker image to use.
* `hostname`: The hostname to be passed to the container.
* `networkMode`: Sets the networking mode for the container. Supported values are: bridge,
   host, and container:&lt;name|id&gt;. See [Docker docs](https://docs.docker.com/reference/run/#network-settings).
* `ports`: Port mapping. Specify an endpoint name and a single port (e.g.
  `{"http": {"internalPort": 8080}}`) for dynamic port mapping and `{"http": {"internalPort": 8080,
  "externalPort": 80}}` for static port mapping. E.g., `{"foo": {"internalPort": 4711}}`  will  map
  the internal port 4711 of the container to an arbitrary external port on the host. Specifying
  `{"foo": {"internalPort": 4711, "externalPort": 80}}` will map internal port 4711  of the
  container to port 80 on the host. The protocol will be TCP by default. For UDP, add
  `"protocol": udp`. E.g. `{"quic": {"internalPort": 80, "protocol": "udp"}}` or
  `{"dns": {"internalPort": 53, "externalPort": 53, "protocol": "udp"}}`. The endpoint name can be
  used when specifying service registration using `registration`. (default: [])
* `registration`: Service discovery registration. Specify a service name, the port name and a protocol
  on the format service/protocol=port. E.g. `{"website/tcp": {"ports": {"http": {}}}}` will register
  the port named http with the protocol tcp. Protocol is optional and default is tcp. If there is
  only one port mapping, this will be used by default and it will be enough to specify only the
  service name, e.g. `{"wordpress": {}}`. (default: {})
* `registrationDomain`: If set, overrides the default domain in which discovery serviceregistration
  occurs. What is allowed here will vary based upon the discovery service plugin used.
  (default: "").
* `securityOpt`: A list of strings denoting security options for running Docker containers,
   i.e. `docker run --security-opt`. See [Docker docs](https://docs.docker.com/reference/run/#security-configuration).
* `token`: Insecure access token meant to prevent accidental changes to your job (e.g. undeploys).
* `volumes`: Container volumes. Specify either a single path to create a data volume, or a source path
  and a container path to mount a file or directory from the host. The container path can be
  suffixed with "rw" or "ro" to create a read-write or read-only volume, respectively.
  Format: `"[container-path]:[rw|ro]":[host-path]`. (default: {}).

A current best practice is to save all your job creation parameters in
version-controlled files in your project directory. This allows you to tie
your Helios job params to changes in your application code.

### Health Checks

When a job is started, Helios can optionally run a health check before registering your service with
service discovery. This prevents the service from receiving traffic before it is ready. After the
container starts, Helios will execute the health check as follows.

* Begin executing health checks and mark the job as "HEALTHCHECKING".
* Start with a 1 second interval, then back off exponentially until reaching a maximum interval of 30 seconds.
* If a health check succeeds
  * Stop running health checks
  * Register service with service discovery (if job is configured to do so)
  * Mark the job as "RUNNING"
* If a health check doesn't succeed, Helios will leave the job in the "HEALTHCHECKING" state forever
  for debugging purposes.

#### HTTP
This health check makes an HTTP request to the specified port and path and considers a return
code of 2xx or 3xx as successful. HTTP health checks are specified in the form `port_name:path`,
where `port_name` is the **name** of the exposed port (as set in the `--port` argument), and `path`
is the path portion of the URL. Requests have a connect timeout of 500ms and a read timeout of 10s.

    helios create --http-check http:health -p http=8080 ...

In your Helios job config file:

```json
"healthCheck" : {
  "type" : "http",
  "path" : "/healthcheck",
  "port" : "http-admin"
}
```

#### TCP
This health check succeeds if it is able to connect to the specified port. You must specify the
**name** of the port as set in the `--port` argument. Each request will timeout after 500ms.

    helios create --tcp-check http-admin -p http-admin=4229 ...

In your Helios job config file:

```json
"healthCheck" : {
  "type" : "tcp",
  "port" : "http-admin"
}
```

#### Exec

This health check runs `docker exec` with the provided command. The service will not be registered
in service discovery until the command executes successfully in the container, i.e. exits with
status code 0.

    helios create --exec-check "ping google.com" ...

In your Helios job config file:

```json
"healthCheck" : {
  "type" : "exec",
  "command" : ["ping", "google.com"]
},
```

### Specifying an Access Token

You can optionally specify an access token when creating a job by using the `--token` parameter.

    $ helios create --token abc123 foo:v1 nginx

You now need to specify the token for each deploy, undeploy, start, stop and remove operation on
that job using `--token` parameter. If no token or an incorrect one is specified, those operations
will fail with a `FORBIDDEN` status code.

**Note:** This token is intended only to prevent unintentional operations. The token mechanism does
not provide actual security, and will not prevent malicious behavior.

Deploying Your Job
---
Now that you've created a Helios job, you can deploy it to Helios hosts. You'll need to find a running Helios host to do so. You can see which hosts are available using the CLI. For example:

    $ helios hosts
    HOST           STATUS         DEPLOYED  RUNNING  CPUS  MEM   LOAD AVG  MEM USAGE  OS                      VERSION  DOCKER
    192.168.33.10. Up 29 minutes  0         0        4     1 gb  0.00      0.39       Linux3.15.3-tinycore64  0.0.33   1.1.2 (1.13)

In the this example, there's a single agent named `192.168.33.10`, so we'll deploy our job there. To deploy the job we can just run:

    $ helios deploy testjob:1 192.168.33.10
    Deploying testjob:1:4f7125bff35d3cecaac237da3ab17efca8a765f9|START on [192.168.33.10]
    192.168.33.10: done

While we only have one host listed here in the `deploy` command, you can specify multiple. While the number of times this will fail should be exceptionally low, the more hosts listed, the higher the probability of failure. But since the operation is idempotent, you can just re-execute it until it succeeds for all hosts. The hosts where the job is already deployed will respond with an error saying `JOB_ALREADY_DEPLOYED`, but that won't prevent the others from being deployed to.

You can deploy more than one job on a host, so deploying another job on the same host will not undeploy the existing one.

### Checking deployment status and history

Now we'd like to see if everything went according to plan:

    $ helios jobs testjob:1
    JOB ID               NAME       VERSION    HOSTS    COMMAND                                             ENVIRONMENT
    testjob:1:4f7125b    testjob    1          1        /bin/sh -c "while true; do date; sleep 60; done"    FOO=bar

    Now we can also see what the history of the job is across all agents if we chose by running:

    $ helios history testjob:1
    AGENT            TIMESTAMP                  STATE       THROTTLED    CONTAINERID
    192.168.33.10    2014-08-11 14:37:44.987    CREATING    NO           <none>
    192.168.33.10    2014-08-11 14:37:45.202    STARTING    NO           60671498ae98
    192.168.33.10    2014-08-11 14:37:45.387    RUNNING     NO           60671498ae98

### Undeploying

We can stop our job by undeploying it:

    $ helios undeploy testjob:1 192.168.33.10
    Undeploying testjob:1:4f7125bff35d3cecaac237da3ab17efca8a765f9 from [192.168.33.10]
    192.168.33.10: done

If we view the history again, it should show something like this:

    $ helios history testjob:1
    AGENT            TIMESTAMP                  STATE       THROTTLED    CONTAINERID
    192.168.33.10    2014-08-11 14:37:44.987    CREATING    NO           <none>
    192.168.33.10    2014-08-11 14:37:45.202    STARTING    NO           60671498ae98
    192.168.33.10    2014-08-11 14:37:45.387    RUNNING     NO           60671498ae98
    192.168.33.10    2014-08-11 14:42:14.403    STOPPED     NO           60671498ae98

We can see that the job stopped. Additionally, checking job status will show again, that the job is stopped.

### Using Deployment Groups

You can manage your deployments at a higher level with deployment groups. A deployment group
determines the list and sequence of hosts to deploy, undeploys prior jobs it has deployed, and
deploys the specified job.

    $ helios create-deployment-group <group name> <label1 key1=value1> <label2 key2=value2>
    $ helios rolling-update <job ID> <group name>
    $ helios deployment-group-status <group name>

[Here's more information](https://github.com/spotify/helios/blob/master/docs/deployment_groups.md)
on how to use deployment groups and their motivation.

Once Inside The Container
---

Now once you are inside your container, if you have exposed ports,
you'll find a few environment variables set that you may need.  If you
have a port named `foo`, there will be an environment variable named
`HELIOS_PORT_foo` set to the host and port of the port named `foo` as
it is seen *from outside the container*.  So if you had `-p foo=2121`
in your job creation commmand, once deployed on a host named
`foo.example.com`, from inside the container you would see
`HELIOS_PORT_foo` set to something like `foo.example.com:23238`
depending on what port was allocated when it was deployed.
