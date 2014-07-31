# -*- mode: ruby -*-
# vi: set ft=ruby :

BOX_NAME = ENV['BOX_NAME'] || "phusion/ubuntu-14.04-amd64"
FORWARD_DOCKER_PORTS = ENV['FORWARD_DOCKER_PORTS']

Vagrant.require_version ">= 1.6.2"

Vagrant.configure("2") do |config|
  # Setup virtual machine box. This VM configuration code is always executed.
  config.vm.box = BOX_NAME

  config.ssh.forward_agent = true
  config.vm.network :forwarded_port, guest: 2375, host: 2375
  config.vm.network :forwarded_port, guest: 5801, host: 5801
  config.vm.network :private_network, ip: "192.168.33.10"

  # sync the maven folder
  config.vm.synced_folder "~/.m2", "/home/vagrant/.m2"

  pkg_cmd = "export DEBIAN_FRONTEND=noninteractive; "
  pkg_cmd = "set -x; "

  # install docker
  pkg_cmd << "curl -s https://get.docker.io/gpg | apt-key add -; "
  pkg_cmd << "echo deb http://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list; "
  pkg_cmd << "apt-get update && apt-get -y install lxc-docker; "

  # Set up docker to listen on 0.0.0.0:2375
  pkg_cmd << "echo 'DOCKER_OPTS=\"--restart=false -D=true -H=tcp://0.0.0.0:2375 -H=unix:///var/run/docker.sock --dns=192.168.33.10\"' > /etc/default/docker; "
  # make docker usable by vagrant user w/o sudo
  pkg_cmd << "groupadd docker; gpasswd -a vagrant docker; service docker restart;"

  # install other helios dependencies and development tools
  pkg_cmd << "apt-get install -y default-jdk maven zookeeperd=3.4.5+dfsg-1 git vim curl golang mercurial; "

  # make sure zk is running
  pkg_cmd << "initctl start zookeeper ;"

  # install helios conf files
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    echo '
    ENABLED=true

    HELIOS_AGENT_OPTS="--state-dir=/var/lib/helios-agent --name=192.168.33.10 --zk localhost:2181 --service-registry http://127.0.0.1:4001 --service-registrar-plugin /usr/share/helios/lib/plugins/helios-skydns-0.1.jar --domain skydns.local"
    ' > /etc/default/helios-agent ;
    END
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    echo '
    ENABLED=true

    HELIOS_MASTER_OPTS="--zk localhost:2181"
    ' > /etc/default/helios-master ;
    END

  # Download and install skydns registrar plugin version 0.1 release from github
  # If you change which version, update the conf file bit above
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    curl -L https://github.com/spotify/helios-skydns/releases/download/0.1/helios-skydns_0.1_all.deb -o helios-skydns_0.1_all.deb && \
    dpkg -i helios-skydns_0.1_all.deb ;
    END

  pkg_cmd << "mkdir -p /home/vagrant/.helios;"
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    echo '{"masterEndpoints":["http://localhost:5801"]}' > /home/vagrant/.helios/config;
    END

  # build and install Helios
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    cd /vagrant && sudo -u vagrant mvn -B -DskipTests package && \
        dpkg --force-confdef --force-confold -i \
            /vagrant/helios-tools/target/*.deb \
            /vagrant/helios-services/target/*.deb ;
    END

  #build skydns version c83f12 to be at least consistent
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    export GOPATH=/home/vagrant/gopath && \
    mkdir -p $GOPATH/src/github.com/skynetservices && \
    cd $GOPATH/src/github.com/skynetservices && \
    git clone https://github.com/skynetservices/skydns.git && \
    cd skydns && \
    git checkout c83f12b96ff3331c0412d7d100ba3e0724f8aa84 && \
    go get -d -v ./... && go build -v ./... && \
    cp skydns /usr/bin/skydns ;
    END

  # build etcd v0.4.5
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    cd /home/vagrant && \
    git clone https://github.com/coreos/etcd && \
    cd etcd && \
    git checkout v0.4.5 && \
    git fetch origin pull/899/head:pull-899 && \
    git merge pull-899 && \
    ./build && \
    cp bin/etcd /usr/bin ;
    END

  # put in upstart config for etcd
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    echo '
    description "etcd"
    
    start on runlevel [2345]
    stop on runlevel [!2345]
    
    respawn
    respawn limit unlimited
    
    script
        [ -r /etc/default/etcd ] && . /etc/default/etcd
        /usr/bin/etcd $ETCD_OPTS
    end script
    
    # prevent respawning more than once every second
    post-stop exec sleep 1
    ' > /etc/init/etcd.conf && \
    initctl start etcd ;
    END

  # put in config for skydns
  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    echo '
    SKYDNS_OPTS="-addr=0.0.0.0:53"
    ' > /etc/default/skydns ;
    END

  pkg_cmd << <<-END.gsub(/^ {4}/, '')
    echo '
    description "skydns"
    
    start on runlevel [2345]
    stop on runlevel [!2345]
    
    respawn
    respawn limit unlimited
    
    script
        [ -r /etc/default/skydns ] && . /etc/default/skydns
        /usr/bin/skydns $SKYDNS_OPTS
    end script
    
    # prevent respawning more than once every second
    post-stop exec sleep 1
    ' > /etc/init/skydns.conf && \
    initctl start skydns ;
  END
  config.vm.provision :shell, :inline => pkg_cmd
end


# Providers were added on Vagrant >= 1.1.0
Vagrant::VERSION >= "1.1.0" and Vagrant.configure("2") do |config|
  config.vm.provider :virtualbox do |vb, override|
    config.vm.box = BOX_NAME
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
  end
end

if !FORWARD_DOCKER_PORTS.nil?
  Vagrant::VERSION < "1.1.0" and Vagrant::Config.run do |config|
    (49000..49900).each do |port|
      config.vm.forward_port port, port
    end
  end

  Vagrant::VERSION >= "1.1.0" and Vagrant.configure("2") do |config|
    (49000..49900).each do |port|
      config.vm.network :forwarded_port, :host => port, :guest => port
    end
  end
end
