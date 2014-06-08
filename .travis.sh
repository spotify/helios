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
    export HOST_IP=`/sbin/ifconfig venet0:0 | grep 'inet addr' | awk -F: '{print $2}' | awk '{print $1}'`

    export DOCKER_HOST=tcp://$HOST_IP:4243
    export DOCKER_PORT_RANGE=4250:4300

    export SLIRP_PORTS=`seq 4243 4300`

    sekexe/run "docker -d -H tcp://0.0.0.0:4243 " &

    while ! docker info; do sleep 1; done

    docker pull busybox
    docker pull rohan/memcached-mini
    ;;
esac
