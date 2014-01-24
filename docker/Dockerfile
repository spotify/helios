# Spotify Helios 0.0.3
#
# docker -H tcp://localhost:4160 run \
#    -h=in_container \
#    -e SPOTIFY_ROLE=helios-agent -e SPOTIFY_DOMAIN=localhost \
#    -e SPOTIFY_SITE=localhost -e ZOOKEEPER=172.17.42.1:2181 \
#    -t -i registry.spotify.net/spotify/helios

FROM       registry.spotify.net/spotify/squeeze-base-java:80a9769
MAINTAINER Drew Csillag "drewc@spotify.com"

ENV DEBIAN_FRONTEND noninteractive

ADD syslog-ng.conf.template /templates/
ADD tmp/jar /usr/share/spotify-helios/lib
ADD tmp/bin /usr/bin/
ADD start /

# add python-argparse which is needed by the start script, plus a few other useful packages
RUN apt-get update && apt-get install -y --force-yes python-argparse vim telnet iputils-ping unzip

ENTRYPOINT ["/usr/bin/python", "-u", "/start"]
