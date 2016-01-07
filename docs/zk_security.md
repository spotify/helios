# ZooKeeper security

To prevent accidental or malicious tampering with the data that Helios stores in ZooKeeper Helios
supports using ZooKeeper's ACL functionality to lock it down (to an extent).  This functionality is
disabled by default.

When enabled, credentials are required to access data in ZooKeeper (we use ZooKeeper's `digest`
authentication scheme). There are two sets of credentials: one set of the masters, and one set for 
the agents. With ACLs enabled unauthenticated users have no access to the data in ZooKeeper (not
even read access). Agents have read access to all data but limited permissions to mutate data. They
only have mutate permissions where needed. This limits the impact of the agent credentials being
compromised (e.g. if an agent is compromised). While the agent credentials being compromised is
obviously not good the lower privileges they provide prevent an attacker from deploying malicious
jobs to other agents (in most cases).

Masters are granted all permissions except ADMIN on all nodes.

**Note that credentials are sent to ZooKeeper in plain-text** (we currently don't support using
encrypted connections to ZooKeeper) - meaing that ACLs are only effective if you can trust the
network that you run on.

## Configuring ACL support

To enable ACL support start the masters with the following options:

    --zk-enable-acls
    --zk-acl-master-user <user> (optional, defaults to helios-master)
    --zk-acl-master-password <password>
    --zk-acl-agent-digest <digest>
    --zk-acl-agent-user <user> (optional, defaults to helios-agent)

And start the agents with the following options:

    --zk-enable-acls
    --zk-acl-master-user <user> (optional, defaults to helios-master)
    --zk-acl-master-digest <digest>
    --zk-acl-agent-user <user> (optional, defaults to helios-agent)
    --zk-acl-agent-password <password>
 
It's recommended to supply the master and agent password using the `HELIOS_ZK_MASTER_PASSWORD` and
`HELIOS_ZK_AGENT_PASSWORD` environment variables respectively, as CLI arguments are visible to any
users on the same host. Environment variables take precedence if both are present.

The digests (`--zk-acl-master-digest`, `--zk-acl-agent-digest`) are base64-encoded SHA1 hashes of
the respective `<username>:<password>` strings. To compute the a digest you can, for example, run
the following command:

    $ echo -n user:password | openssl dgst -sha1 -binary | base64
    tpUq/4Pn5A64fVZyQ0gOJ8ZWqkY=

## Migrating an existing cluster to using ACLs

ACLs are applied only to new nodes as they are created. Enabling ACL support on an existing cluster
will not break anything but ACLs will not be retroactively applied to already existing ZK nodes.

TODO