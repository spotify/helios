Helios Event Senders
===

There is support to send events on Helios cluster updates.

From helios masters, the following events are sent:

Topic `HeliosDeploymentGroupEvents` with a `DeploymentGroupEvent` message,
see `com.spotify.helios.rollingupdate.DeploymentGroupEventFactory`.

From helios agents:

Topic `HeliosTaskStatusEvents` with a `HeliosTaskStatusEvent` message,
see `com.spotify.helios.common.descriptors.TaskStatusEvent`.


There is support to send events over Kafka, and over Google PubSub.


Kafka
---

Using the command line argument `--kafka <broker>`, Kafka publishing is enabled. The Kafka sender creates a KafkaRecord from the
topic and message as is.

GooglePubSub
---

Using the command line argument `--pubsub-topic-prefix <topic-prefix>`, Google PubSub publishing is enabled. The Google PubSub sender
concatenates the topic prefix with the the helios event topic (with no separator), and sends the message as payload.
