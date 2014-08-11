Reviewed by [philipcristiano](https://github.com/philipcristiano) on 2014-05-13

***

This guide gives an overview of Helios, and what you need to know to deploy and run your Docker containers using it.

Note that this guide assumes that you are familiar with [Docker](http://docker.io) and concepts like images and containers. If you aren't familiar with Docker, see the [getting started page](https://www.docker.io/gettingstarted/).

Basic Concepts in Helios
---

* **Job:** A job configuration tells Helios how to run your Docker container. It consists of things like a job name, a job version, the name of your Docker image, any environment variables you wish to pass to the running container, ports you wish to expose, and the command to run inside the container, if any.

* **Master:** Helios masters are the servers that the Helios CLI and other tools talk to. They coordinate the deployment of your Docker containers onto Helios agents.

* **Agents:** Helios agents, sometimes known as Helios hosts, are the machines on which the images you built eventually run. The masters tell agents to download jobs and run them.

Using the Helios CLI
---

The `helios` command is your primary interface for interacting with the Helios cluster.

When using the CLI, you'll have to specify which Helios endpoint you want to talk to. To talk to a specific Helios master, use the `-z` flag. For example, if you have a Helios master running locally on the default port:

    helios -z http://localhost:5801

If you have multiple masters, you can [setup automatic master lookup](automatic_master_lookup.md).

**Note:** that the rest of this manual uses the `helios` command without specifying either the `-z` or `-s` flag. This is only for simplicity, though you can emulate this behavior by aliasing the `helios` command. For example:

    alias helios=helios -z http://localhost:5801

This will save you from having to type the `-z` flag over and over again.

If you need general help, run `helios --help`. Many Helios CLI commands also take the `--json` option, which will produce JSON results instead of human-readable ones. This is useful if you want to script Helios or incorporate it into a build process.

Creating Your Job
---
Once you have a Docker image you want to deploy, you need to tell Helios how you want it run. First we tell Helios the relevant details.

### A basic job

If you specified an `ENTRYPOINT` in the `Dockerfile` you built your image with, you can create your job with the `create` command. For example, to create a job named "foo" to run the [rohan/memcached-tiny](https://index.docker.io/u/rohan/memcached-tiny/) image:

    $ helios create foo:v1 rohan/memcached-tiny
    Creating job: {"id":"foo:v1:2a89d5a87851d68678aabdad38d31faa296f5bf1","image":"rohan/memcached-tiny","command":[],"env":{},"ports":{},"registration":{}}
    Done.
    foo:v1:2a89d5a87851d68678aabdad38d31faa296f5bf1

### Specifying a command to run

If you didn't specify an `ENTRYPOINT` in your Dockerfile, you can specify a command to run when the container is started. For example, to create a Ubuntu container that prints the time every 60 seconds:

    $ helios create testjob:1 ubuntu:12.04 -- \
        /bin/sh -c 'while true; do date; sleep 60; done'
    Creating job: {"id":"testjob:1:9689a0350f3655c1cb2342a4230eb1418b64d
    cc5","image":"ubuntu:12.04","command":["/bin/sh","-c","while true; do dat
    e; sleep 60; done"],"env":{}}
    Done.
    testjob:1:9689a0350f3655c1cb2342a4230eb1418b64dcc5

**NOTE**: When passing a command-line to your container, precede it with the double dash (`--`) as in the above example. While it might work sometimes without it, I wouldn't count on it, as the Helios CLI may misinterpert your intent.

### Passing environment variables

For some use cases, you may want to pass some environment variables to the job that aren't baked into the image. In which case you can do:

    $ helios create testjob 1 ubuntu:12.04 --env FOO=bar -- \
        /bin/sh -c 'while true; do echo $FOO; date; sleep 60; done'
    Creating job: {"id":"testjob:1:bad9111c6c9e10975408f2f41c561fd3849f55
    61","image":"ubuntu:12.04","command":["/bin/sh","-c","while true; do echo $FOO; date; sleep 60;
    done"],"env":{"FOO":"bar"}}
    Done.
    testjob:1:bad9111c6c9e10975408f2f41c561fd3849f5561

The last line of output in the command output is the canonical job ID. Most times, you will only need the `jobName:jobVersion` parts, but in the event that you create two jobs with the same name and version, you can unambiguously choose which one you intend to operate on by supplying the full ID.

As a current best practice, it is advised to put your `create` command lines into version-controlled files in your project directory, one file per create statement. This way, when you go to do subsequent job creations and deployments, you've got a record of what you did the last time.

Deploying Your Job
---
Now that you've created a Helios job, you can deploy it to Helios hosts. You'll need to find a running Helios host to do so. You can see which hosts are available using the CLI. For example:

    $ helios hosts
    HOST                    STATUS          DEPLOYED    RUNNING    CPUS     MEM     LOAD AVG    MEM USAGE    OS       VERSION
    192.168.33.10    Up 39 minutes 49 seconds    0           0          2       0 gb    0.00        0.83         Linux    3.13.0-24-generic

In the this example, there's a single agent named `192.168.33.10`, so we'll deploy our job there. To deploy the job we can just run:

    $ helios deploy testjob:1 192.168.33.10
    Deploying Deployment{jobId=testjob:1:bad9111c6c9e10975408f2f41c561fd3849f5561, goal=START} on [192.168.33.10]
    192.168.33.10: done

While we only have one host listed here in the `deploy` command, you can specify multiple. While the number of times this will fail should be exceptionally low, the more hosts listed, the higher the probability of failure. But since the operation is idempotent, you can just re-execute it until it succeeds for all hosts. The hosts where the job is already deployed will respond with an error saying `JOB_ALREADY_DEPLOYED`, but that won't prevent the others from being deployed to.

You can deploy more than one job on a host, so deploying another job on the same host will not undeploy the existing one.

### Checking deployment status and history

Now we'd like to see if everything went according to plan:

    $ helios job testjob:1
    JOB ID                                                 HOST                    STATE      CONTAINER ID     COMMAND                                                       THROTTLED?    ENVIRONMENT
    testjob:1:bad9111c6c9e10975408f2f41c561fd3849f5561    192.168.33.10    RUNNING    60671498ae98    /bin/sh -c while true; do echo $FOO; date; sleep 60; done    NO            FOO=bar

This is a little hard to read because of the line wrapping, but the `STATE` of `RUNNING` is the key thing to note above. It would appear that our job is running just fine. Now we can also see what the history of the job is across all agents if we chose by running:

    $ helios history testjob:1
    AGENT                   TIMESTAMP                  STATE       THROTTLED    CONTAINERID
    192.168.33.10    2013-11-27 14:37:44.987    CREATING    NO           <none>
    192.168.33.10    2013-11-27 14:37:45.202    STARTING    NO           60671498ae98
    192.168.33.10    2013-11-27 14:37:45.387    RUNNING     NO           60671498ae98

### Undeploying

We can stop our job by undeploying it:

    $ helios undeploy testjob:1 192.168.33.10
    Undeploying testjob:1:bad9111c6c9e10975408f2f41c561fd3849f5561 from [192.168.33.10]
    192.168.33.10: done

If we view the history again, it should show something like this:

    $ helios history testjob:1
    AGENT                   TIMESTAMP                  STATE       THROTTLED    CONTAINERID
    192.168.33.10    2013-11-27 14:37:44.987    CREATING    NO           <none>
    192.168.33.10    2013-11-27 14:37:45.202    STARTING    NO           60671498ae98
    192.168.33.10    2013-11-27 14:37:45.387    RUNNING     NO           60671498ae98
    192.168.33.10    2013-11-27 14:42:14.403    STOPPED     NO           60671498ae98

We can see that the job stopped. Additionally, checking job status will show again, that the job is stopped.
