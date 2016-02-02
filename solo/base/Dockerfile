FROM ubuntu:trusty

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl dnsutils zookeeper git mercurial \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install helios-skydns plugin
ENV SKYDNS_PLUGIN_VERSION 0.1
ENV SKYDNS_PLUGIN_DEB helios-skydns_${SKYDNS_PLUGIN_VERSION}_all.deb
ENV SKYDNS_PLUGIN_DEB_URI https://github.com/spotify/helios-skydns/releases/download/$SKYDNS_PLUGIN_VERSION/$SKYDNS_PLUGIN_DEB
RUN curl -o $SKYDNS_PLUGIN_DEB -L $SKYDNS_PLUGIN_DEB_URI \
    && dpkg -i $SKYDNS_PLUGIN_DEB \
    && rm $SKYDNS_PLUGIN_DEB

# Install Go (from the official golang Dockerfile)
ENV GOLANG_VERSION 1.4.2
ENV PATH /go/bin:/usr/local/go/bin:$PATH
ENV GOPATH /go

RUN curl -sSL https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz \
    | tar -C /usr/local -xz
RUN mkdir -p /go/src /go/bin && chmod -R 777 /go

# Install skydns
RUN mkdir -p /go/src/github.com/skynetservices
RUN cd /go/src/github.com/skynetservices \
    && git clone https://github.com/skynetservices/skydns.git \
    && cd skydns && go get -v && go build

# Install etcd
ENV ETCD_ARCHIVE_URI https://github.com/coreos/etcd/releases/download/v2.0.5/etcd-v2.0.5-linux-amd64.tar.gz
RUN mkdir /etcd \
    && cd /etcd \
    && curl -L $ETCD_ARCHIVE_URI | tar --strip-components 1 -xzvf - \
    && cp ./etcd /usr/bin

# Install Java 8 from webupd8team PPA
RUN \
  echo deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main > /etc/apt/sources.list.d/webupd8team-java-trusty.list && \
  apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886 && \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  apt-get update && \
  apt-get install -y --no-install-recommends oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer