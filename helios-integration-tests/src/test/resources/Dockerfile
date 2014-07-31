# This generates an image which is used by TemporaryJobsITCase. The test needs dig and
# netcat-traditional, so this will install the dnsutils and netcat-traditional packages.

FROM ubuntu:14.04
RUN apt-get update && apt-get install -y dnsutils netcat-traditional && update-alternatives --set nc /bin/nc.traditional
