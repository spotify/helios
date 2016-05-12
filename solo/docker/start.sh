#!/bin/bash -xe

# Look up the ip address of
IPADDRESS=$(ip addr | grep inet | grep eth0 | tr '/' ' ' |  awk '{ print $2 }')

# Start etcd
etcd $ETCD_OPTS &

NAMESERVERS=$(cat /etc/resolv.conf | grep nameserver |
              python -c "import json, sys; ns=['%s:53' % (l.strip().split()[1], ) for l in sys.stdin]; print json.dumps(ns or ['8.8.8.8:53', '8.8.4.4:53']);")
SKYDNS_PATH=$(echo $HELIOS_NAME|python -c "import sys;h=sys.stdin.read().strip().rstrip('.').split('.');h.reverse();print '/'.join(h)")

# Write skydns configuration and retry for 30 seconds until successful
for i in {1..30}; do
  if curl --retry 30 -XPUT http://127.0.0.1:4001/v2/keys/skydns/config \
    -d value="{\"dns_addr\":\"0.0.0.0:5353\", \"ttl\":3600, \"nameservers\": $NAMESERVERS, \"domain\":\"local.\"}"; then
    break
  fi
  sleep 1
done

# Create A record for the solo host
curl -XPUT http://127.0.0.1:4001/v2/keys/skydns/${SKYDNS_PATH} \
    -d value="{\"host\":\"$HOST_ADDRESS\"}"

skydns $SKYDNS_OPTS &
unbound

/usr/share/zookeeper/bin/zkServer.sh start

# Start agent
# SPOTIFY_POD and SPOTIFY_DOMAIN must be overridden to <job prefix>.local by the
# TemporaryJobs config files in order for service discovery to work.
mkdir -p /agent
cd /agent
java -cp '/*' \
-Xmx128m \
-Djava.net.preferIPv4Stack=true \
com.spotify.helios.agent.AgentMain \
--name ${HELIOS_NAME} \
--service-registrar-plugin /usr/share/helios/lib/plugins/helios-skydns-0.1.jar \
--id ${HELIOS_ID:-solo-host} \
--dns $IPADDRESS \
--domain 'local.' \
--service-registry "http://127.0.0.1:4001" \
--env SPOTIFY_POD='local.' \
--env SPOTIFY_DOMAIN='local.' \
--env HELIOS_HOST_ADDRESS=$HOST_ADDRESS \
--labels solo=yes \
$HELIOS_AGENT_OPTS \
&

# Start master
mkdir -p /master
if [ -n "$LOGSTASH_DESTINATION" ]; then
  cat > /master/logback-access.xml <<- EOF
<configuration>
  <appender name="stash" class="net.logstash.logback.appender.LogstashAccessTcpSocketAppender">
    <destination>${LOGSTASH_DESTINATION}</destination>

    <!-- encoder is required -->
    <encoder class="net.logstash.logback.encoder.LogstashAccessEncoder">
      <fieldNames>
        <fieldsRequestHeaders>@fields.request_headers</fieldsRequestHeaders>
        <fieldsResponseHeaders>@fields.response_headers</fieldsResponseHeaders>
      </fieldNames>
    </encoder>
  </appender>

  <appender-ref ref="stash" />
</configuration>
EOF
fi

cd /master
java -cp '/*' \
-Xmx128m \
-Djava.net.preferIPv4Stack=true \
com.spotify.helios.master.MasterMain \
--service-registrar-plugin /usr/share/helios/lib/plugins/helios-skydns-0.1.jar \
--domain '' \
$HELIOS_MASTER_OPTS \
&

if [ "$HELIOS_SOLO_SUICIDE" -eq 1 ]; then
    HELIOS_CLI_CMD="java -cp /*.jar -Xmx128m -Djava.net.preferIPv4Stack=true com.spotify.helios.cli.CliMain -z http://localhost:5801"

    python "/watchdog.py"

    echo "Cleaning up jobs..."

    # Clean up created jobs (make sure containers are stopped)
    jobs=$($HELIOS_CLI_CMD --json jobs | jq -a -r "keys[]")
    for job in $jobs; do
        $HELIOS_CLI_CMD undeploy --yes -a "$job"
    done

    # Wait for jobs to be undeployed
    for job in $jobs; do
        states=$($HELIOS_CLI_CMD status --json -j "$job" | jq -r ".[].taskStatuses | .[] | .state")
        tot=$(echo "$states" | wc -l)
        stopped=$(echo "$states" | grep "STOPPED" | wc -l)
        if [ "$stopped" -eq "$tot" ]; then
            echo "All instances of $job are stopped"
            break
        fi
    done

    echo -e "Some containers may still be running at this point, but they will eventually be killed.
             Undeploying Helios jobs makes Helios tell Docker to stop the container by sending
             SIGTERM and to wait two minutes before sending SIGKILL (same as docker kill)."
    echo "Clean up done. Exiting"
else
    set +x

    # Sleep or execute command line
    while :; do sleep 1; done
fi
