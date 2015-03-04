Reviewed by [davidxia](https://github.com/davidxia) on 2014-11-01

***

This guide gives an overview of Helios, and what you need to know to deploy and run your Docker containers using it.

Note that this guide assumes that you are familiar with [Docker](http://docker.io) and concepts like images and containers. If you aren't familiar with Docker, see the [getting started page](https://www.docker.io/gettingstarted/).


* [Basic Concepts in Helios](#basic-concepts-in-helios)
* [Install the Helios CLI](#install-the-helios-cli)
* [Using the Helios CLI](#using-the-helios-cli)
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

    helios -z http://localhost:5801

If you have multiple masters, you can [setup automatic master lookup](automatic_master_lookup.md).

**Note:** that the rest of this manual uses the `helios` command without specifying either the `-z` or `-d` flag. This is only for simplicity, though you can emulate this behavior by aliasing the `helios` command. For example:

    alias helios=helios -z http://localhost:5801

This will save you from having to type the `-z` flag over and over again.

If you need general help, run `helios --help`. Many Helios CLI commands also take the `--json` option, which will produce JSON results instead of human-readable ones. This is useful if you want to script Helios or incorporate it into a build process.

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
output of `helios inspect -d <DOMAINS> <EXISTING_JOB_NAME> --json`. Here's an example:

```
{
  "command" : [ "foo", "bar" ],
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
  "volumes" : {
    "/etc/foo/moar-config.yaml:ro" : "/etc/bar/moar-config.yaml"
  }
}
```

A current best practice is to save all your job creation parameters in
version-controlled files in your project directory. This allows you to tie
your helios job params to changes in your application code.

### Health Checks

When a job is started, Helios can optionally run a health check before registering your service with
nameless. This prevents the service from receiving traffic before it is ready. After the container
starts, Helios will execute the health check as follows.

* Begin executing health checks, using exponential backoff from 1 second to 30 seconds.
* If no health checks succeed after 5 minutes, kill the container. Helios will start it back up and try again.
* If a health check succeeds
  * Stop running health checks
  * Register service with nameless (if job is configured to do so)
  * Mark the job as RUNNING

#### HTTP
This health check will make an http request to the specified port and path, and consider a return
code of 2xx or 3xx as successful. HTTP health checks are specified in the form `port_name:path`,
where `port_name` is the **name** of the exposed port (as set in the `--port` argument), and `path`
is the path portion of the URL. Requests have a connect timeout of 500ms and a read timeout of 10s.

    helios create --http-check http:health -p http=8080 -r foo/http=http

#### TCP
This health check is successful if it able to connect to the specified port. You must specify the
**name** of the port as set in the `--port` argument. Each request will timeout after 500ms.

    helios create --tcp-check hm -p hm=4229 -r foo/hm=hm

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
