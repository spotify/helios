# -*- mode: ruby -*-
# vi: set ft=ruby :

BOX_NAME = ENV['BOX_NAME'] || "ubuntu"
BOX_URI = ENV['BOX_URI'] || "http://files.vagrantup.com/precise64.box"
VF_BOX_URI = ENV['BOX_URI'] || "http://files.vagrantup.com/precise64_vmware_fusion.box"
AWS_REGION = ENV['AWS_REGION'] || "us-east-1"
AWS_AMI    = ENV['AWS_AMI']    || "ami-d0f89fb9"
FORWARD_DOCKER_PORTS = ENV['FORWARD_DOCKER_PORTS']

Vagrant.configure("2") do |config|
  # Setup virtual machine box. This VM configuration code is always executed.
  config.vm.box = BOX_NAME
  config.vm.box_url = BOX_URI

  config.ssh.forward_agent = true
  config.vm.network :forwarded_port, guest: 4160, host: 4160
  config.vm.network :private_network, ip: "192.168.33.10"

  # Provision docker and new kernel if deployment was not done.
  # It is assumed Vagrant can successfully launch the provider instance.
  if Dir.glob("#{File.dirname(__FILE__)}/.vagrant/machines/default/*/id").empty?
    pkg_cmd = "export DEBIAN_FRONTEND=noninteractive; "

    # Add Ubuntu raring backported kernel
    pkg_cmd << "apt-get update -qq; apt-get install -q -y linux-image-generic-lts-raring; "

    # Add guest additions if local vbox VM. As virtualbox is the default provider,
    # it is assumed it won't be explicitly stated.
    if ENV["VAGRANT_DEFAULT_PROVIDER"].nil? && ARGV.none? { |arg| arg.downcase.start_with?("--provider") }
      pkg_cmd << "apt-get install -q -y linux-headers-generic-lts-raring dkms; " \
        "echo 'Downloading VBox Guest Additions...'; " \
        "wget -q http://dlc.sun.com.edgesuite.net/virtualbox/4.2.12/VBoxGuestAdditions_4.2.12.iso; "

      # Prepare the VM to add guest additions after reboot
      pkg_cmd << "echo -e 'mount -o loop,ro /home/vagrant/VBoxGuestAdditions_4.2.12.iso /mnt\n" \
        "echo yes | /mnt/VBoxLinuxAdditions.run\numount /mnt\n" \
          "rm /root/guest_additions.sh; ' > /root/guest_additions.sh; " \
        "chmod 700 /root/guest_additions.sh; " \
        "sed -i -E 's#^exit 0#[ -x /root/guest_additions.sh ] \\&\\& /root/guest_additions.sh#' /etc/rc.local; "
    end

    # Set up the spotify0 network bridge
    pkg_cmd << <<-END.gsub(/^ {6}/, '')
      echo "
      auto spotify0
         iface spotify0 inet static
         bridge_ports none
         bridge_stp off
         bridge_fd 0
         bridge_waitport 0
         netmask 255.255.0.0
         address 10.99.0.1
      " >> /etc/network/interfaces
      END

    # Use our dns
    pkg_cmd << <<-END.gsub(/^ {6}/, '')
      echo "\
      domain spotify.net
      search spotify.net.
      nameserver 193.182.13.186
      nameserver 193.182.13.179
      " > /etc/resolv.conf
      END
    pkg_cmd << <<-END.gsub(/^ {6}/, '')
      echo "\
      domain spotify.net
      search spotify.net.
      " > /etc/resolvconf/resolv.conf.d/base
      END

    # Fire up syslog on a tcp port
    pkg_cmd << <<-END.gsub(/^ {6}/, '')
      echo "\
      \\$ModLoad imtcp # needs to be done just once
      \\$InputTCPMaxSessions 500
      \\$InputTCPServerRun 6514
      " > /etc/rsyslog.d/1-tcp.conf
      END

    # Use spotify apt sources
    pkg_cmd << "apt-get --allow-unauthenticated update && apt-get --allow-unauthenticated install --force-yes -y apt-utils apt-transport-https; "
    pkg_cmd << <<-END.gsub(/^ {6}/, '')
      echo "\
      # precise-backports
      deb http://debmirror:9999/ubuntu/ precise-backports main restricted universe
      deb-src http://debmirror:9999/ubuntu/ precise-backports main restricted universe

      # precise-security
      deb http://debmirror:9999/ubuntu-security/ precise-security main restricted universe
      deb-src http://debmirror:9999/ubuntu-security/ precise-security main restricted universe

      # precise-standard
      deb http://debmirror:9999/ubuntu/ precise main restricted universe
      deb-src http://debmirror:9999/ubuntu/ precise main restricted universe

      # precise-updates
      deb http://debmirror:9999/ubuntu/ precise-updates main restricted universe
      deb-src http://debmirror:9999/ubuntu/ precise-updates main restricted universe

      # spotify-private-stable
      deb http://spotify:fuph3AM0ti3ohXugu5ip@debmirror:9444/precise/debian/ stable main non-free
      deb-src http://spotify:fuph3AM0ti3ohXugu5ip@debmirror:9444/precise/debian/ stable main non-free

      # spotify-private-testing
      deb http://spotify:fuph3AM0ti3ohXugu5ip@debmirror:9444/precise/debian/ testing main non-free
      deb-src http://spotify:fuph3AM0ti3ohXugu5ip@debmirror:9444/precise/debian/ testing main non-free

      # spotify-private-unstable
      deb http://spotify:fuph3AM0ti3ohXugu5ip@debmirror:9444/precise/debian/ unstable main non-free
      deb-src http://spotify:fuph3AM0ti3ohXugu5ip@debmirror:9444/precise/debian/ unstable main non-free
      " > /etc/apt/sources.list ;
      END
    pkg_cmd << "apt-get --allow-unauthenticated update && apt-get --allow-unauthenticated install --force-yes -y spotify-apt-keys; "
    pkg_cmd << "apt-get --allow-unauthenticated update; "

    # Add lxc-docker package
    pkg_cmd << "apt-get --allow-unauthenticated install -qq --force-yes lxc-docker=0.8.0-1~0.0.0.6092.ac8cada.106; "

    # Add syslog-redirector package
    pkg_cmd << "apt-get --allow-unauthenticated install -qq --force-yes syslog-redirector; "

    # Set up docker to listen on 0.0.0.0:4160 and to use the spotify0 network bridge
    pkg_cmd << "echo 'DOCKER_OPTS=\"-D=true -H=tcp://0.0.0.0:4160 -H=unix:///var/run/docker.sock -b=spotify0\"' > /etc/default/docker; "

    # Add vagrant user to the docker group
    # pkg_cmd << "usermod -a -G docker vagrant; "

    # Activate new kernel
    pkg_cmd << "shutdown -r +1; "

    config.vm.provision :shell, :inline => pkg_cmd
  end
end


# Providers were added on Vagrant >= 1.1.0
Vagrant::VERSION >= "1.1.0" and Vagrant.configure("2") do |config|
  config.vm.provider :aws do |aws, override|
    aws.access_key_id = ENV["AWS_ACCESS_KEY_ID"]
    aws.secret_access_key = ENV["AWS_SECRET_ACCESS_KEY"]
    aws.keypair_name = ENV["AWS_KEYPAIR_NAME"]
    override.ssh.private_key_path = ENV["AWS_SSH_PRIVKEY"]
    override.ssh.username = "ubuntu"
    aws.region = AWS_REGION
    aws.ami    = AWS_AMI
    aws.instance_type = "t1.micro"
  end

  config.vm.provider :rackspace do |rs|
    config.ssh.private_key_path = ENV["RS_PRIVATE_KEY"]
    rs.username = ENV["RS_USERNAME"]
    rs.api_key  = ENV["RS_API_KEY"]
    rs.public_key_path = ENV["RS_PUBLIC_KEY"]
    rs.flavor   = /512MB/
    rs.image    = /Ubuntu/
  end

  config.vm.provider :vmware_fusion do |f, override|
    override.vm.box = BOX_NAME
    override.vm.box_url = VF_BOX_URI
    override.vm.synced_folder ".", "/vagrant", disabled: true
    f.vmx["displayName"] = "docker"
  end

  config.vm.provider :virtualbox do |vb|
    config.vm.box = BOX_NAME
    config.vm.box_url = BOX_URI
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
