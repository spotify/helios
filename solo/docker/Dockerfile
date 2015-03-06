FROM netflixoss/java:7

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y curl zookeeper

# Install helios-skydns plugin
ENV SKYDNS_PLUGIN_VERSION 0.1
ENV SKYDNS_PLUGIN_DEB helios-skydns_${SKYDNS_PLUGIN_VERSION}_all.deb
ENV SKYDNS_PLUGIN_DEB_URI https://github.com/spotify/helios-skydns/releases/download/$SKYDNS_PLUGIN_VERSION/$SKYDNS_PLUGIN_DEB
RUN curl -o $SKYDNS_PLUGIN_DEB -L $SKYDNS_PLUGIN_DEB_URI \
    && dpkg -i $SKYDNS_PLUGIN_DEB

# Install Go (from the official golang Dockerfile)
ENV GOLANG_VERSION 1.3.3
ENV PATH /go/bin:/usr/src/go/bin:$PATH
ENV GOPATH /go

RUN apt-get install -y --no-install-recommends gcc libc6-dev make
RUN curl -sSL https://golang.org/dl/go$GOLANG_VERSION.src.tar.gz \
    | tar v -C /usr/src -xz
RUN cd /usr/src/go/src && ./make.bash --no-clean 2>&1
RUN mkdir -p /go/src /go/bin && chmod -R 777 /go

RUN apt-get install -y git mercurial

# Install etcd
RUN git clone https://github.com/coreos/etcd
WORKDIR /etcd
RUN git checkout v0.4.5
RUN git fetch origin pull/899/head:pull-899 && git merge pull-899
RUN ./build && cp bin/etcd /usr/bin

# Install skydns
RUN mkdir -p /go/src/github.com/skynetservices
WORKDIR /go/src/github.com/skynetservices
RUN git clone https://github.com/skynetservices/skydns.git
RUN cd skydns && git checkout 829cce8 && go get && go build

EXPOSE 5801

WORKDIR /
ADD start.sh /
ADD *.jar /
ENTRYPOINT ["/start.sh"]
