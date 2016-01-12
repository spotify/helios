FROM spotify/helios-solo-base:0.2

EXPOSE 5801
EXPOSE 33333

RUN apt-get update
RUN apt-get install jq
ADD start.sh /
ADD watchdog.py /
ADD *.jar /
ENTRYPOINT ["/start.sh"]
