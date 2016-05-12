FROM spotify/helios-solo-base:0.6

EXPOSE 5801
EXPOSE 33333

# Move to solo-base/Dockerfile?
RUN apt-get update
RUN apt-get install jq

ADD start.sh /
ADD *.jar /
ADD watchdog.py /
ENTRYPOINT ["/start.sh"]
