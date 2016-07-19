# Deployment Groups

A deployment group is a set of hosts with configuration and state on the masters that helps you deploy
Helios jobs to agents more easily. It determines the list and sequence of hosts to deploy,
undeploys prior jobs it has deployed, and deploys the specified job.

See the [original proposal here](https://github.com/spotify/helios/issues/502) for background and
motivation.

## Usage

### Create

A host selector is an expression of the form [label] [operator] [operand].
Host selectors are used by deployment groups to choose which hosts to act on. The command below will
create a deployment group whose hosts match all the specified host selectors.

    $ helios create-deployment-group foo-group foo=bar baz=qux

    Creating deployment group: {"labels":{"baz":"qux","foo":"bar"},"name":"foo-group"}
    {"status":"CREATED"}

Here's a list of the available types of host selectors:

  * [label] = foo
  * [label] != foo
  * [label] in (foo, bar, qux, ...)
  * [label] notin (foo, bar, qux, ...)

Creating a deployment group with the same name and labels as an existing one will succeed with a
"not modified" response. Creating a deployment group with the same name but different labels as an
existing one will return an error stating that the deployment group already exists.

### Updating the definition

To change the definition of a deployment group, such as adding/removing host
selectors, you will need to remove the group, create it again with the same
name and new host selectors, and then run `rolling-update --migrate` again.

### Inspect

Inspect the deployment group:

    $ helios inspect-deployment-group foo-group

    Name: foo-group
    Labels: baz=qux
            foo=bar
    Job:

### List

List deployment groups:

    $ helios list-deployment-groups

    foo-group

### Rolling Update

Tell the deployment group to deploy job foo:0.1.0 to all its agents.

    $ helios rolling-update foo:0.1.0 foo-group

    Rolling update started: foo-group -> foo:0.1.0 (parallelism=1, timeout=300)

    host1 -> RUNNING (1/3)
    host2 -> RUNNING (2/3)
    host3 -> RUNNING (3/3)

    Done.
    Duration: 4.00 s

This command makes a deployment request and blocks until the update is complete. Helios determines
the list and sequence of agents, undeploys any prior jobs this deployment group had deployed, and
deploys the new job. If the job fails to reach the RUNNING state on any agent, the rolling update
is stopped and marked as FAILED.

If the update succeeds it is marked as ACTIVE. Helios will periodically recalculate the list of
agents that match the deployment group labels and make sure they are all running the specified job.
This means new agents that match the labels will automatically be told to deploy the job.

See more rolling-update options with `helios rolling-update -h`, e.g. parallel deployments and
failure timeouts.

### Status

Check on the status:

    $ helios deployment-group-status foo-group

    Name: foo-group
    Job Id: foo-job:0.1.0
    Status: ROLLING_OUT

    HOST      UP-TO-DATE    JOB              STATE
    host1.    X             foo-job:0.1.0    RUNNING
    host2.    X             foo-job:0.1.0    PULLING_IMAGE
    host3.                  -                -

See more deployment group commands with `helios -h`.

  [1]: https://github.com/spotify/helios/blob/master/docs/user_manual.md#label-agents
