#!/bin/bash

case "$1" in
  before_install)
    sudo sh -c "wget -qO- https://get.docker.io/gpg | apt-key add -"
    sudo sh -c "echo deb http://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
    sudo apt-get update

    echo exit 101 | sudo tee /usr/sbin/policy-rc.d
    sudo chmod +x /usr/sbin/policy-rc.d
    sudo apt-get install -qy slirp lxc lxc-docker

    git clone git://github.com/spotify/sekexe
    ;;

  before_script)
    export MAVEN_OPTS="-Xmx128m"

    export HOST_IP=`/sbin/ifconfig venet0:0 | grep 'inet addr' | awk -F: '{print $2}' | awk '{print $1}'`

    export DOCKER_HOST=tcp://$HOST_IP:2375
    export DOCKER_PORT_RANGE=2400:2500

    export SLIRP_PORTS=`seq 2375 2500`

    sekexe/run "docker -d -H tcp://0.0.0.0:2375 " &

    while ! docker info; do sleep 1; done
    ;;

  before_deploy)
    tar --xform s:^.*/:: -czf debs.tgz helios-tools/target/*.{deb,changes} helios-services/target/*.{deb,changes}
    tar -ztvf debs.tgz
    ;;

  after_deploy)
    python .travis.addServer.py
    mvn clean deploy -DskipTests --settings ~/.m2/mySettings.xml
    ;;

esac
